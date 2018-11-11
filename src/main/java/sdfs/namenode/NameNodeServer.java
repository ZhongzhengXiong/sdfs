/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;
import sdfs.datanode.DataNodeServer;
import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.filetree.*;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;


public class NameNodeServer implements INameNodeProtocol, INameNodeDataNodeProtocol {
    public static final int NAME_NODE_PORT = 4343;
    private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
    private final Map<UUID, FileNode> readwritePFile = new HashMap<>();
    private final Map<UUID, FileNode> readwriteFileCopy = new HashMap<>();

    String workingPath = "NameNodeFile/";
    PriorityQueue<Integer> freeBlocks = new PriorityQueue<Integer>();
    private DirNode root;
    public NameNodeServer() {
        initial();
    }



    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {

        try {
            FileNode fileNode = (FileNode) traverse(fileUri);
            if(readwritePFile.containsValue(fileNode)){
                UUID ruuid = UUID.randomUUID();
                Set<Map.Entry<UUID, FileNode>>  rwSet= readwritePFile.entrySet();
                for (Map.Entry<UUID, FileNode> e:
                     rwSet) {
                    if(e.getValue().equals(fileNode)){
                        ruuid = e.getKey();
                        break;
                    }
                }
                fileNode = readwriteFileCopy.get(ruuid);
            }
            UUID uuid = UUID.randomUUID();
            readonlyFile.put(uuid, fileNode);
            return new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, true);

        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return null;
    }


    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException {
        try {
            FileNode fileNode = (FileNode) traverse(fileUri);
            UUID uuid = UUID.randomUUID();
            if(readwritePFile.containsValue(fileNode))
                throw new OverlappingFileLockException();
            if (readwritePFile.containsKey(uuid))
                throw new IllegalStateException();
            readwritePFile.put(uuid, fileNode);
            FileNode fileNodeCopy = (FileNode)fileNode.deepClone();
            readwriteFileCopy.put(uuid,fileNodeCopy);
            return new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, false);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }


    public Node traverse(String fileUri) throws IOException, URISyntaxException {
        String[] parseResult = parseUri(fileUri);
        Node tmpNode = root;
        String name;
        Iterator<Entry> iter;
        boolean founded;
        int len = parseResult.length;

        for (int i = 0; i < len; i++) {
            founded = false;
            name = parseResult[i];
            if (tmpNode instanceof FileNode)
                throw new IOException("it's not a directory");

            iter = ((DirNode) tmpNode).iterator();
            while (iter.hasNext()) {
                Entry entry = iter.next();
                if (entry.getName().equals(name)) {
                    founded = true;
                    tmpNode = entry.getNode();
                    break;
                }

            }
            if (!founded) {
                throw new FileNotFoundException();
            }
        }
        return tmpNode;

    }


    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        try {
            FileNode fileNode = (FileNode) createNode(fileUri, 0);
            UUID uuid = UUID.randomUUID();
            readwritePFile.put(uuid, fileNode);
            FileNode fileNodeCopy = (FileNode)fileNode.deepClone();
            readwriteFileCopy.put(uuid,fileNodeCopy);
            return new SDFSFileChannel(uuid, fileNode.getFileSize(), fileNode.getBlockAmount(), fileNode, false);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
        readonlyFile.remove(fileUuid);
    }


    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        FileNode fileNode = readwritePFile.remove(fileUuid);
        readwriteFileCopy.remove(fileUuid);
        //if new file size not in (blockAmount * BLOCK_SIZE, (blockAmount + 1) * BLOCK_SIZE]
        int blockAmount = fileNode.getBlockAmount();
        if (!(newFileSize > ((blockAmount - 1) * DataNodeServer.BLOCK_SIZE) && newFileSize <= (blockAmount * DataNodeServer.BLOCK_SIZE)))
            throw new IllegalArgumentException();
        fileNode.setFileSize(newFileSize);
        saveFileTree();
    }


    @Override
    public void mkdir(String fileUri) throws IOException {
        try {
            createNode(fileUri, 1);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


    @Override
    public LocatedBlock addBlock(UUID fileUuid) throws IllegalStateException {
        if (readonlyFile.containsKey(fileUuid))
            throw new IllegalStateException();
        FileNode fileNode = readwritePFile.get(fileUuid);
        try {
            int blockNumber = getBlockNumber();
            saveMetadata();
            InetAddress inetAddress = InetAddress.getByName("localhost");
            LocatedBlock locatedBlock = new LocatedBlock(inetAddress, blockNumber);
            BlockInfo blockInfo = new BlockInfo();
            blockInfo.addLocatedBlock(locatedBlock);
            fileNode.addBlockInfo(blockInfo);
            saveFileTree();
            return locatedBlock;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {

        List<LocatedBlock> blocks = new ArrayList<LocatedBlock>(blockAmount);
        for (int i = 0; i < blockAmount; i++) {
            blocks.add(addBlock(fileUuid));
        }
        return blocks;
    }


    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        if (readonlyFile.containsKey(fileUuid))
            throw new IllegalStateException();
        FileNode fileNode = readwritePFile.get(fileUuid);
        fileNode.removeLastBlockInfo();
        int blockNumber = fileNode.getLastBlockInfo().iterator().next().getBlockNumber();
        freeBlocks.add(blockNumber);
        try {
            saveMetadata();
        } catch (IOException e) {
            e.printStackTrace();
        }
        saveFileTree();
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        for (int i = 0; i < blockAmount; i++) {
            removeLastBlock(fileUuid);
        }
    }

    public Node createNode(String fileUri, int type) throws URISyntaxException, IOException {
        String[] parseResult = parseUri(fileUri);
        Node tmpNode = root;
        String name;
        Iterator<Entry> iter;
        boolean isExist;
        int len = parseResult.length;
        int i;
        //find first directory
        for (i = 0; i < len; i++) {
            isExist = false;
            name = parseResult[i];
            if (tmpNode instanceof FileNode)
                throw new IOException("it's not a directory");
            iter = ((DirNode) tmpNode).iterator();
            while (iter.hasNext()) {
                Entry entry = iter.next();
                if (entry.getName().equals(name)) {
                    isExist = true;
                    tmpNode = entry.getNode();
                    break;
                }
            }
            if (!isExist && i != len - 1)
                throw new FileNotFoundException();
            if (isExist && i == len - 1)
                throw new SDFSFileAlreadyExistException();

        }
        DirNode parentNode = (DirNode) tmpNode;
        name = parseResult[len - 1];
        if (type == 0) {
            FileNode fileNode = new FileNode();
            parentNode.addEntry(new Entry(name, fileNode));
            saveFileTree();
            return fileNode;
        } else {
            DirNode dirNode = new DirNode();
            parentNode.addEntry(new Entry(name, dirNode));
            saveFileTree();
        }
        return null;
    }



    public DirNode list(String fileUri) throws URISyntaxException, IOException {
        Node node = traverse(fileUri);
        if(node instanceof FileNode)
            throw new IOException("it's not a directory");
        return (DirNode)node;
    }




    public void saveFileTree() {
        try {
            File file = new File("NameNodeFile/fsimage");
            if (!file.exists())
                file.createNewFile();
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
            out.writeObject(root);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void initial() {
        try {
            File file0 = new File("NameNodeFile/");
            if (!file0.exists())
                file0.mkdir();
            File file = new File("NameNodeFile/fsimage");
            if (!file.exists()) {
                file.createNewFile();
                root = new DirNode();
                saveFileTree();
            } else {
                ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
                root = (DirNode) in.readObject();
                in.close();
            }

            File file1 = new File("NameNodeFile/metadata");
            if (!file1.exists()) {
                file1.createNewFile();
                freeBlocks.add(0);
                saveMetadata();
            } else {
                ObjectInputStream in1 = new ObjectInputStream(new FileInputStream(file1));
                freeBlocks = (PriorityQueue<Integer>) in1.readObject();
                in1.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    public int getBlockNumber() throws IOException {
        int blockNumber = freeBlocks.poll();
        if (freeBlocks.size() == 0) {
            freeBlocks.add((blockNumber + 1));
        }
        saveMetadata();
        return blockNumber;
    }

    public void saveMetadata() throws IOException {
        File file = new File(workingPath + "metadata");
        FileOutputStream outputStream = new FileOutputStream(file);
        ObjectOutputStream obj_out = new ObjectOutputStream(outputStream);
        obj_out.writeObject(freeBlocks);
    }


    public String[] parseUri(String fileUri) throws URISyntaxException {
        fileUri.trim();
        int len = fileUri.length();

        if(len == 0){
            String[] finalResult = {};
            return finalResult;
        }
        String splitString = fileUri;
        if (fileUri.charAt(len - 1) == '/')
            splitString = fileUri.substring(0, len - 1);
        String[] finalResult = splitString.split("/");
        return finalResult;
    }

    public static void main(String args[]) throws IOException {
        NameNodeServer nameNodeServer = new NameNodeServer();
        ServerSocket serverSocket = new ServerSocket(NAME_NODE_PORT);
        while (true) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                ServerThread thread = new ServerThread(socket, nameNodeServer);
                thread.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class ServerThread extends Thread {

    Socket socket;
    NameNodeServer nameNodeServer;

    public ServerThread(Socket socket, NameNodeServer nameNodeServer) {
        this.socket = socket;
        this.nameNodeServer = nameNodeServer;
    }

    public Socket getSocket() {
        return socket;
    }

    public void run() {
        try {
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
//            System.out.println(1);
            String method = (String) in.readObject();
            Class<?>[] paraType = (Class<?>[]) in.readObject();
            Object[] paras = (Object[]) in.readObject();

            Class<?> cla = Class.forName("sdfs.namenode.NameNodeServer");
            Method m = cla.getMethod(method, paraType);

            try {
                Object result = m.invoke(nameNodeServer, paras);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(result);
            } catch (InvocationTargetException e) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(e.getCause());
            }

        } catch (IOException | ClassNotFoundException | IllegalAccessException | NoSuchMethodException e) {
            e.printStackTrace();
        } finally {
            try {
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
