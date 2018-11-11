/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.client.DataNodeStub;
import sdfs.client.NameNodeStub;
import sdfs.datanode.DataNodeServer;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

import static java.lang.Math.min;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;
    private final UUID uuid; //File uuid
    private int fileSize; //Size of this file
    private int blockAmount; //Total block amount of this file
    private final FileNode fileNode;
    private final boolean isReadOnly;
    private final HashMap<LocatedBlock, byte[]> dataBlocksCache = new LinkedHashMap<>(16, 0.75f, true); //BlockNumber to DataBlock cache. byte[] or ByteBuffer are both acceptable.
    private NameNodeStub nameNodeStub;
    private boolean closed = false;
    private int position = 0;
    private int fileDataBlockCacheSize;

    SDFSFileChannel(UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly) {
        this.uuid = uuid;
        this.fileSize = fileSize;
        this.blockAmount = blockAmount;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;

    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        //todo your code here
        if(closed)
            throw new ClosedChannelException();
        if(fileSize <= position)
            return 0;
        int pos0 = dst.position();
        Iterator<BlockInfo> iter = fileNode.iterator();
        int blkSize = DataNodeServer.BLOCK_SIZE;
        byte[] blockData;
        for(int i = 0; i < (position / blkSize) && iter.hasNext(); i++)
            iter.next();
        while(iter.hasNext()){
            BlockInfo blockInfo = iter.next();
            LocatedBlock locatedBlock = blockInfo.iterator().next();
            //compute the number of bytes to be readed
            int readNum = min(min(dst.limit() - dst.position(), blkSize - position % blkSize), fileSize - position);
            //load into cache if cache missed
            if(!dataBlocksCache.containsKey(locatedBlock)){
                InetAddress inetAddress = locatedBlock.getInetAddress();
                int blockNumber = locatedBlock.getBlockNumber();
                DataNodeStub dataNodeStub = new DataNodeStub(inetAddress);
                blockData = dataNodeStub.read(uuid, blockNumber, 0, blkSize);
                //LRU
                if(dataBlocksCache.size() == fileDataBlockCacheSize){
                    LocatedBlock rblock = dataBlocksCache.keySet().iterator().next();
                    byte[] rdata = dataBlocksCache.remove(locatedBlock);
                }
                putIntoCache(locatedBlock, blockData);
            }
            blockData = dataBlocksCache.get(locatedBlock);
            dst.put(blockData, position % blkSize, readNum);
            position += readNum;
            //bytebuffer is full or reach the end of file stream
            if(dst.limit() == dst.position() || position == fileSize)
                return dst.position() - pos0;
        }
        return 0;
    }

    public void putIntoCache(LocatedBlock locatedBlock, byte[] blockData) throws IOException {
        if(dataBlocksCache.size() == fileDataBlockCacheSize && !isReadOnly){
            LocatedBlock rblock = dataBlocksCache.keySet().iterator().next();
            byte[] rdata = dataBlocksCache.remove(rblock);
            InetAddress inetAddress = locatedBlock.getInetAddress();
            DataNodeStub dataNodeStub = new DataNodeStub(inetAddress);
            if(fileNode.blockAmount > 0 && rblock.equals(fileNode.getLastBlockInfo().iterator().next())){
                int lastBlkSize = fileSize - (getBlockAmount(fileSize) - 1)* DataNodeServer.BLOCK_SIZE;
                byte[] wdata = new byte[lastBlkSize];
                System.arraycopy(rdata, 0, wdata, 0, wdata.length);
                dataNodeStub.write(uuid, rblock.getBlockNumber(), 0, wdata);
            }else{
                dataNodeStub.write(uuid, rblock.getBlockNumber(), 0, rdata);
            }
        }
        dataBlocksCache.put(locatedBlock, blockData);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        //todo your code here
        if(closed)
            throw new ClosedChannelException();
        if(isReadOnly)
            throw new NonWritableChannelException();

        int pos0 = src.position();
        Iterator<BlockInfo> iter = fileNode.iterator();
        int blkSize = DataNodeServer.BLOCK_SIZE;
        byte[] blockData = new byte[blkSize];

        if(position > fileSize){
            int blockAmount = (getBlockAmount(position)) - (getBlockAmount(fileSize));
            LocatedBlock lastBlock = fileNode.getLastBlockInfo().iterator().next();
            List<LocatedBlock> locatedBlocks = nameNodeStub.addBlocks(uuid, blockAmount);
            //update local fileNode
            for(int i = 0; i < blockAmount; i++){
                BlockInfo blockInfo = new BlockInfo();
                blockInfo.addLocatedBlock(locatedBlocks.get(i));
                fileNode.addBlockInfo(blockInfo);
            }
            byte[] zeroData = new byte[blkSize];

           // LocatedBlock lastBlock = fileNode.getLastBlockInfo().iterator().next();
            if(!dataBlocksCache.containsKey(lastBlock)){
                InetAddress inetAddress = lastBlock.getInetAddress();
                DataNodeStub dataNodeStub = new DataNodeStub(inetAddress);
                byte[] zeroAdded = dataNodeStub.read(uuid, lastBlock.getBlockNumber(), 0, blkSize);
                putIntoCache(lastBlock, zeroAdded);
            }
            for(int i = 0; i < blockAmount; i++){
                LocatedBlock locatedBlock = locatedBlocks.get(i);
                if(dataBlocksCache.containsKey(locatedBlock))
                    dataBlocksCache.replace(locatedBlock, zeroData);
                else
                    putIntoCache(locatedBlock, zeroData);
            }
            fileSize = position;
        }



        for(int i = 0; i < (position / blkSize) && iter.hasNext(); i++)
            iter.next();
        while(iter.hasNext()){
            BlockInfo blockInfo = iter.next();
            LocatedBlock locatedBlock = blockInfo.iterator().next();
            //compute write num
            int writeNum = min(blkSize - position % blkSize, src.limit() - src.position());
            //cache hit
            if(dataBlocksCache.containsKey(locatedBlock)){
                blockData = dataBlocksCache.get(locatedBlock);
                src.get(blockData, position % blkSize, writeNum);
                dataBlocksCache.replace(locatedBlock, blockData);
            }else{
                //cache miss
                InetAddress inetAddress = locatedBlock.getInetAddress();
                DataNodeStub dataNodeStub = new DataNodeStub(inetAddress);
                blockData = dataNodeStub.read(uuid, locatedBlock.getBlockNumber(), 0, blkSize);
                src.get(blockData, position % blkSize, writeNum);
                putIntoCache(locatedBlock, blockData);
            }
            position += writeNum;
            fileSize = position;
            if(src.limit() == src.position())
                return src.position() - pos0;
        }


        int blockAmount = (src.limit() - src.position() - 1) / blkSize + 1;
        List<LocatedBlock> locatedBlocks = nameNodeStub.addBlocks(uuid, blockAmount);
        for(int i = 0; i < blockAmount; i++){
            BlockInfo blockInfo = new BlockInfo();
            blockInfo.addLocatedBlock(locatedBlocks.get(i));
            fileNode.addBlockInfo(blockInfo);
        }
        for(int i = 0; i < blockAmount; i++){
            LocatedBlock locatedBlock = locatedBlocks.get(i);
            int writeNum = min(blkSize - position % blkSize, src.limit() - src.position());
            src.get(blockData, position % blkSize, writeNum);
            position += writeNum;
            fileSize = position;
            if(dataBlocksCache.containsKey(locatedBlock))
                dataBlocksCache.replace(locatedBlock, blockData);
            else
                putIntoCache(locatedBlock, blockData);
        }
        return src.position() - pos0;
    }

    @Override
    public long position() throws IOException {
        //todo your code here
        if(closed)
            throw new ClosedChannelException();
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        //todo your code here
        if(closed)
            throw new ClosedChannelException();
        if(newPosition < 0)
            throw new IllegalArgumentException();
        position = (int)newPosition;
        return null;
    }

    @Override
    public long size() throws IOException {
        //todo your code here
        if(closed)
            throw new ClosedChannelException();
        return fileSize;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        //todo your code here
        if(closed)
            throw new ClosedChannelException();
        if(isReadOnly)
            throw new NonWritableChannelException();
        if(size < 0)
            throw new IllegalArgumentException();
        if(position > size)
            position = (int)size;
        if(size >= this.size())
            return this;
        //long blockAmount = ((fileSize -1) / DataNodeServer.BLOCK_SIZE + 1) - ((size -1) / DataNodeServer.BLOCK_SIZE + 1);
        long blockAmount = getBlockAmount(fileSize) - getBlockAmount((int)size);
        for(int i = 0; i < blockAmount; i++){
            nameNodeStub.removeLastBlock(uuid);
            fileNode.removeLastBlockInfo();
        }
        fileSize = (int)size;
        this.blockAmount -= blockAmount;
        return this;
    }

    @Override
    public boolean isOpen() {
        //todo your code here
        return !closed;
    }

    @Override
    public void close() throws IOException {
        //todo your code here
        if(closed)
            return;
        if(!isReadOnly){
            nameNodeStub.closeReadwriteFile(uuid, fileSize);
            flush();
        }else{
            nameNodeStub.closeReadonlyFile(uuid);
        }
        closed = true;
    }

    @Override
    public void flush() throws IOException {
        //todo your code here
        if(closed)
            throw new ClosedChannelException();
        if(isReadOnly)
            throw new NonWritableChannelException();
        Iterator<LocatedBlock> iterator = dataBlocksCache.keySet().iterator();

        Set<Map.Entry<LocatedBlock, byte[]>> dcache = dataBlocksCache.entrySet();
        for(Map.Entry<LocatedBlock, byte[]> e :
           dcache  ) {
            LocatedBlock locatedBlock = e.getKey();
            byte[] dataBlock = e.getValue();
            InetAddress inetAddress = locatedBlock.getInetAddress();
            DataNodeStub dataNodeStub = new DataNodeStub(inetAddress);
            if(fileNode.blockAmount > 0 && locatedBlock.equals(fileNode.getLastBlockInfo().iterator().next())){
                int lastBlkSize = fileSize - (getBlockAmount(fileSize) - 1)* DataNodeServer.BLOCK_SIZE;
                byte[] wdata = new byte[lastBlkSize];
                System.arraycopy(dataBlock, 0, wdata, 0, lastBlkSize);
                dataNodeStub.write(uuid, locatedBlock.getBlockNumber(), 0, wdata);
            }else{
                dataNodeStub.write(uuid, locatedBlock.getBlockNumber(), 0, dataBlock);
            }
        }
        dataBlocksCache.clear();
    }


    public void fix(NameNodeStub nameNodeStub, int fileDataBlockCacheSize){
        this.nameNodeStub = nameNodeStub;
        this.fileDataBlockCacheSize = fileDataBlockCacheSize;
    }

    private static int getBlockAmount(int fileSize) {
        return (fileSize == 0 ? 0 : (((fileSize - 1) / DataNodeServer.BLOCK_SIZE) + 1));
    }
}






