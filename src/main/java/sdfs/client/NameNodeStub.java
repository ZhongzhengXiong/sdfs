/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.SDFSFileChannel;
import sdfs.protocol.INameNodeProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.channels.OverlappingFileLockException;
import java.util.List;
import java.util.UUID;

public class NameNodeStub implements INameNodeProtocol {


    InetSocketAddress nameNodeAddress;

    public NameNodeStub(InetSocketAddress nameNodeAddress){
        this.nameNodeAddress = nameNodeAddress;
    }



    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        String method = "openReadonly";
        Class<?>[] paraType = {String.class};
        Object[] paras = {fileUri};
        Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
        if(object instanceof IOException)
            throw (IOException)object;
        if(object instanceof  SDFSFileChannel)
            return (SDFSFileChannel)object;
        //SDFSFileChannel sdfsFileChannel = (SDFSFileChannel)SdfsRmi.send(nameNodeAddress, method, paraType, paras);
       // return sdfsFileChannel;
        return null;
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException{
        String method = "openReadwrite";
        Class<?>[] paraType = {String.class};
        Object[] paras = {fileUri};
        Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
        if(object instanceof IndexOutOfBoundsException)
            throw (IndexOutOfBoundsException)object;
        if(object instanceof OverlappingFileLockException)
            throw (OverlappingFileLockException)object;
        if(object instanceof IllegalStateException)
            throw (IllegalStateException)object;
        if(object instanceof IOException)
            throw (IOException)object;
        if(object instanceof  SDFSFileChannel)
            return (SDFSFileChannel)object;
        return null;

    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IllegalStateException, IOException {
        String method = "create";
        Class<?>[] paraType = {String.class};
        Object[] paras = {fileUri};
        Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
        if(object instanceof FileNotFoundException)
            throw (FileNotFoundException)object;
        if(object instanceof SDFSFileAlreadyExistException)
            throw (SDFSFileAlreadyExistException)object;
        if(object instanceof IllegalStateException)
            throw (IllegalStateException)object;
        if(object instanceof  SDFSFileChannel)
            return (SDFSFileChannel)object;
        return null;
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
        String method = "closeReadonlyFile";
        Class<?>[] paraType = {UUID.class};
        Object[] paras = {fileUuid};
        Object object =  SdfsRmi.send(nameNodeAddress, method, paraType, paras);
        if(object instanceof IllegalStateException)
            throw (IllegalStateException)object;
        if(object instanceof IOException)
            throw (IOException)object;

    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        String method = "closeReadwriteFile";
        Class<?>[] paraType = {UUID.class, int.class};
        Object[] paras = {fileUuid, newFileSize};
        Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
        if(object instanceof IndexOutOfBoundsException)
            throw (IndexOutOfBoundsException)object;
        if(object instanceof IllegalStateException)
            throw (IllegalStateException)object;
        if(object instanceof IOException)
            throw (IOException)object;
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        String method = "mkdir";
        Class<?>[] paraType = {String.class};
        Object[] paras = {fileUri};
        Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
        if(object instanceof IOException)
            throw (IOException)object;
    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) {
        String method = "addBlock";
        Class<?>[] paraType = {UUID.class};
        Object[] paras = {fileUuid};
        try {
            LocatedBlock locatedBlock = (LocatedBlock)SdfsRmi.send(nameNodeAddress, method, paraType, paras);
            return locatedBlock;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        String method = "addBlocks";
        Class<?>[] paraType = {UUID.class, int.class};
        Object[] paras = {fileUuid, blockAmount};
        try {
            List<LocatedBlock> locatedBlocks = (List<LocatedBlock>)SdfsRmi.send(nameNodeAddress, method, paraType, paras);
            return locatedBlocks;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        String method = "removeLastBlock";
        Class<?>[] paraType = {UUID.class};
        Object[] paras = {fileUuid};
        try {
            Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
            if(object instanceof IllegalStateException)
                throw (IllegalStateException)object;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        String method = "removeLastBlocks";
        Class<?>[] paraType = {UUID.class, int.class};
        Object[] paras = {fileUuid, blockAmount};
        try {
            Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
            if(object instanceof IllegalStateException)
                throw (IllegalStateException)object;
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void delete(String fileUri) throws IOException {
        String method = "delete";
        Class<?>[] paraType = {String.class};
        Object[] paras = {fileUri};
        Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
        if(object instanceof IOException)
            throw (IOException)object;
    }

    public Object list(String fileUri) throws IOException{
        String method = "list";
        Class<?>[] paraType = {String.class};
        Object[] paras = {fileUri};
        Object object = SdfsRmi.send(nameNodeAddress, method, paraType, paras);
        if(object instanceof IOException)
            throw (IOException)object;
        return object;
    }

}
