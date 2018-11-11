/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import sdfs.datanode.DataNodeServer;
import sdfs.namenode.LocatedBlock;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileNode extends Node implements Serializable, Iterable<BlockInfo> {
    private static final long serialVersionUID = -5007570814999866661L;
    private final List<BlockInfo> blockInfos = new ArrayList<>();
    private int fileSize;//file size should be checked when closing the file.
    public int blockAmount = blockInfos.size();

    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfos.add(blockInfo);
        blockAmount++;
    }

    public void removeLastBlockInfo() {
        blockInfos.remove(blockInfos.size() - 1);
        blockAmount--;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
        blockAmount = (fileSize == 0 ? 0 : (((fileSize - 1) / DataNodeServer.BLOCK_SIZE) + 1));
    }

    public int getBlockAmount() {
        return blockInfos.size();
    }

    @Override
    public Iterator<BlockInfo> iterator() {
        return blockInfos.listIterator();
    }


    public BlockInfo getLastBlockInfo() {
        return blockInfos.get(blockInfos.size() - 1);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;

        return blockInfos.equals(that.blockInfos);
    }

    @Override
    public int hashCode() {
        return blockInfos.hashCode();
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }


    public Object deepClone() throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bo);
        oo.writeObject(this);//从流里读出来
        ByteArrayInputStream bi = new ByteArrayInputStream(bo.toByteArray());
        ObjectInputStream oi = new ObjectInputStream(bi);
        return (oi.readObject());
    }


}

