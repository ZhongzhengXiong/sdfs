/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.filetree.DirNode;
import sdfs.filetree.Entry;
import sdfs.filetree.Node;
import sdfs.namenode.SDFSFileChannel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SDFSClient implements ISimpleDistributedFileSystem {
    /**
     * @param fileDataBlockCacheSize Buffer size for file data block. By default, it should be 16.
     * That means 16 block of data will be cache on local.
     * And you should use LRU algorithm to replace it.
     * It may be change during test. So don't assert it will equal to a constant.
     */

    InetSocketAddress nameNodeAddress;
    int fileDataBlockCacheSize;
    public NameNodeStub nameNodeStub;


    public SDFSClient(InetSocketAddress nameNodeAddress, int fileDataBlockCacheSize) {
        //todo your code here

        this.nameNodeAddress = nameNodeAddress;
        this.fileDataBlockCacheSize = fileDataBlockCacheSize;
        nameNodeStub = new NameNodeStub(nameNodeAddress);
    }


    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        //todo your code here
        try {
            fileUri = parseUri(fileUri);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        SDFSFileChannel sdfsFileChannel = nameNodeStub.openReadonly(fileUri);
        sdfsFileChannel.fix(nameNodeStub, fileDataBlockCacheSize);
        return sdfsFileChannel;
    }


    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        //todo your code here
        try {
            fileUri = parseUri(fileUri);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        SDFSFileChannel sdfsFileChannel = nameNodeStub.create(fileUri);
        sdfsFileChannel.fix(nameNodeStub, fileDataBlockCacheSize);
        return sdfsFileChannel;
    }


    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        //todo your code here
        try {
            fileUri = parseUri(fileUri);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        SDFSFileChannel sdfsFileChannel = nameNodeStub.openReadwrite(fileUri);
        sdfsFileChannel.fix(nameNodeStub, fileDataBlockCacheSize);
        return sdfsFileChannel;
    }


    @Override
    public void mkdir(String fileUri) throws IOException {
        //todo your code here
        try {
            fileUri = parseUri(fileUri);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        nameNodeStub.mkdir(fileUri);
    }


    public String parseUri(String fileUri) throws URISyntaxException {
        String regex = "^(sdfs|SDFS)://((([0-9]+.)+[0-9]+)|localhost)(:[0-9]+)*(/[\\s\\S]*)+(/)*$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(fileUri);
        if (!matcher.matches()) {
            throw new URISyntaxException(fileUri, "wrong format");
        }
        //parse the string based on '/'
        int index1 = fileUri.indexOf('/');
        int index2 = fileUri.indexOf("/", index1 + 2);
        String filePath = fileUri.substring(index2 + 1);
        return filePath;
        // return fileUri;
    }


    public void list(String fileUri) throws IOException, URISyntaxException {
        fileUri = parseUri(fileUri);
        DirNode dirNode = (DirNode) nameNodeStub.list(fileUri);
        Iterator<Entry> iter = dirNode.iterator();
        String type;
        while(iter.hasNext()){
            Entry entry = iter.next();
            if(entry.getNode() instanceof DirNode)
                type = "Dir";
            else
                type = "File";
            System.out.println(type + "  " + entry.getName());
        }
    }

    public void delete(String fileUri) throws IOException, URISyntaxException {
        fileUri = parseUri(fileUri);
        nameNodeStub.delete(fileUri);
    }



    public static void main(String args[]) throws IOException, URISyntaxException {
        InetSocketAddress nameNodeAddress = new InetSocketAddress("localhost", 4343);
        SDFSClient client = new SDFSClient(nameNodeAddress, 16);

        if (args[0].equals("get")) {
            String lpath = args[1];
            String rpath = args[2];
            SDFSFileChannel sdfsFileChannel = client.openReadonly(rpath);
            ByteBuffer dst = ByteBuffer.allocate(1024);
            int len = 0;
            FileOutputStream out = new FileOutputStream(lpath);
            while ((len = sdfsFileChannel.read(dst)) > 0) {
                dst.flip();
                dst.limit(len);
                out.getChannel().write(dst);
                dst.clear();
            }
            out.close();

        } else if (args[0].equals("put")) {
            String lpath = args[1];
            String rpath = args[2];
            File file = new File(lpath);
            FileInputStream in = new FileInputStream(file);
            FileChannel fileChannel = in.getChannel();
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            int len = 0;
            SDFSFileChannel sdfsFileChannel = client.create(rpath);
            while ((len = fileChannel.read(byteBuffer)) > 0) {
                byteBuffer.flip();
                byteBuffer.limit(len);
                sdfsFileChannel.write(byteBuffer);
                byteBuffer.clear();
            }
            sdfsFileChannel.close();
            fileChannel.close();
        } else if (args[0].equals("mkdir")) {
            String rpath = args[1];
            client.mkdir(rpath);
        } else if (args[0].equals("list")) {
            String rpath = args[1];
            client.list(rpath);
        }

    }

}
