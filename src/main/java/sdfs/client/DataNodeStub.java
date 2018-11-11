/*
 * Copyright (c) Jipzingking 2016.
 */

//package sdfs.client;

import sdfs.protocol.IDataNodeProtocol;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;

public class DataNodeStub implements IDataNodeProtocol {

    InetSocketAddress dataNodeAddress;


    public DataNodeStub(InetAddress inetAddress){
        this.dataNodeAddress = new InetSocketAddress(inetAddress, 4341);
    }


    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {
        String method = "read";
        Class<?>[] paraType = {UUID.class, int.class, int.class, int.class};
        Object[] paras = {fileUuid, blockNumber, offset, size};
        byte[] b = (byte[]) sdfs.client.SdfsRmi.send(dataNodeAddress, method, paraType, paras);
        return b;
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {

        String method = "write";
        Class<?>[] paraType = {UUID.class, int.class, int.class, byte[].class};
        Object[] paras = {fileUuid, blockNumber, offset, b};
        sdfs.client.SdfsRmi.send(dataNodeAddress, method, paraType, paras);
    }
}
