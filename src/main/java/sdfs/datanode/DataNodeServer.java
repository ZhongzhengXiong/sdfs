/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.datanode;

import sdfs.protocol.IDataNodeProtocol;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.PriorityQueue;
import java.util.UUID;

public class DataNodeServer implements IDataNodeProtocol {
    /**
     * The block size may be changed during test.
     * So please use this constant.
     */
    public static final int BLOCK_SIZE = 128 * 1024;
    public static final int DATA_NODE_PORT = 4341;
    String workingDir = "DataNodeFile/";


    public DataNodeServer() {
        //initial dirs
        File dir = new File(workingDir);
        if (!dir.exists())
            dir.mkdirs();
    }


    //    put off due to its difficulties
    //    private final Map<UUID, Set<Integer>> uuidReadonlyPermissionCache = new HashMap<>();
    //    private final Map<UUID, Set<Integer>> uuidReadwritePermissionCache = new HashMap<>();

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {

        byte[] b = new byte[size];
        File file = new File(workingDir + blockNumber + ".block");

        //check whether file exists
        if (!file.exists()) {
            throw new FileNotFoundException();
        }

        //check IndexOutOfBoundsException
        long file_size = file.length();
        if (offset < 0 || offset > BLOCK_SIZE) {
            throw new IndexOutOfBoundsException();
        }

        //read data into byte array
        FileInputStream in = new FileInputStream(file);
        in.skip(offset);
        int rsize = in.read(b, 0, size);

        return b;

    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {
        File file = new File(workingDir + blockNumber + ".block");
        if (offset < 0 || offset > BLOCK_SIZE) {
            throw new IndexOutOfBoundsException();
        }
        RandomAccessFile out = new RandomAccessFile(file, "rw");
        out.skipBytes(offset);
        out.write(b, 0, b.length);
        out.close();
    }

    public static void main(String args[]) throws IOException {
        DataNodeServer dataNodeServer = new DataNodeServer();
        ServerSocket serverSocket = new ServerSocket(DATA_NODE_PORT);
        while (true) {
            Socket socket = null;
            try {
                socket = serverSocket.accept();
                ServerThread thread = new ServerThread(socket, dataNodeServer);
                thread.start();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}

class ServerThread extends Thread {

    Socket socket;
    DataNodeServer dataNodeServer;

    public ServerThread(Socket socket, DataNodeServer dataNodeServer) {
        this.socket = socket;
        this.dataNodeServer = dataNodeServer;
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

            Class<?> cla = Class.forName("sdfs.datanode.DataNodeServer");
            Method m = cla.getMethod(method, paraType);
            Object result = m.invoke(dataNodeServer, paras);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(result);

        } catch (IOException | ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
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



