/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client

import org.apache.commons.lang3.RandomStringUtils
import sdfs.datanode.DataNodeServer
import sdfs.namenode.NameNodeServer
import sdfs.protocol.INameNodeDataNodeProtocol
import sdfs.protocol.INameNodeProtocol
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.OverlappingFileLockException
import java.rmi.registry.LocateRegistry
import java.rmi.registry.Registry
import java.rmi.server.UnicastRemoteObject

class SDFSClientTest extends Specification {
    @Shared
    def FILE_SIZE = 2 * DataNodeServer.BLOCK_SIZE + 2
    @Shared
    def dataBuffer = ByteBuffer.allocate(FILE_SIZE)
    @Shared
    def buffer = ByteBuffer.allocate(FILE_SIZE)
    def parentDir = generateFilename()
    def filename = parentDir + "/" + generateFilename()
    @Shared
    SDFSClient client = new SDFSClient(new InetSocketAddress("localhost", 4343), 16 )
    @Shared
    NameNodeServer nameNodeServer
    @Shared
    DataNodeServer dataNodeServer

    def setupSpec() {
        System.setProperty("user.dir", File.createTempDir().absolutePath);
        for (int i = 0; i < FILE_SIZE; i++)
            dataBuffer.put(i.byteValue())
    }

    def cleanupSpec() {
        for (def file : new File(".").listFiles(new FilenameFilter() {
            @Override
            boolean accept(File dir, String name) {
                return name.endsWith(".block") || name.endsWith(".log") || name.endsWith(".node")
            }
        })) {
            file.delete()
        }
    }

    def setup() {
        client.mkdir(parentDir)
    }

    private def writeFileChannel() {
        def fc = client.create(filename)
        dataBuffer.position(0)
        fc.write(dataBuffer)
        fc.close()
    }

    /**
     * Not necessary to pass
     * Only Preview for lab 3
     */
    def "Test create"() {
        def fileSize = FILE_SIZE

        when:
        def fc = client.create("$filename")
        dataBuffer.position(0)

        then:
        fc.write(dataBuffer) == fileSize
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == fileSize
        fc.write(dataBuffer) == 0

        when:
        fc.truncate(fileSize + 1)

        then:
        fc.position() == fileSize
        fc.size() == fileSize

        when:
        client.openReadWrite(filename)

        then:
        thrown OverlappingFileLockException

        when:
        def fc2 = client.openReadonly(filename)

        then:
        fc2.size() == 0
        fc2.fileNode.blockAmount == getBlockAmount(0)
        fc2.position() == 0

        when:
        fc.close()

        then:
        fc2.size() == 0
        fc2.fileNode.blockAmount == getBlockAmount(0)
        fc2.position() == 0

        when:
        fc = client.openReadonly(filename)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        cleanup:
        fc.close()
        fc2.close()
    }

    /**
     * Not necessary to pass
     * Only Preview for lab 3
     */
    def "Test truncate"() {
        def fileSize = FILE_SIZE
        writeFileChannel()

        when:
        def fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        when:
        fc.truncate(0)
        buffer.position(0)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.position() == 0
        fc.read(buffer) == 0

        when:
        def fc2 = client.openReadonly(filename)

        then:
        fc2.size() == fileSize
        fc2.fileNode.blockAmount == getBlockAmount(fileSize)
        fc2.position() == 0

        when:
        fc.close()

        then:
        fc2.size() == fileSize
        fc2.fileNode.blockAmount == getBlockAmount(fileSize)
        fc2.position() == 0

        when:
        fc = client.openReadWrite(filename)
        dataBuffer.position(0)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.position() == 0
        fc.read(buffer) == 0
        fc.write(dataBuffer) == fileSize

        cleanup:
        fc.close()
        fc2.close()
    }

    def "Test append data"() {
        def fileSize = FILE_SIZE
        def secondPosition = 3 * DataNodeServer.BLOCK_SIZE - 1
        writeFileChannel()

        when:
        def fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        when:
        fc.position(secondPosition)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == secondPosition
        fc.read(buffer) == 0
        fc.write(dataBuffer) == 0

        when:
        dataBuffer.position(0)

        then:
        fc.write(dataBuffer) == fileSize
        fc.size() == secondPosition + fileSize
        fc.fileNode.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == secondPosition + fileSize
        fc.read(buffer) == 0

        when:
        fc.position(0)

        then:
        fc.read(buffer) == fileSize
        fc.position() == fileSize
        fc.size() == secondPosition + fileSize
        buffer == dataBuffer
        fc.read(buffer) == 0
        fc.size() == secondPosition + fileSize
        fc.fileNode.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == fileSize

        when:
        fc.truncate(secondPosition + fileSize + 1)

        then:
        fc.size() == secondPosition + fileSize
        fc.fileNode.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == fileSize

        when:
        buffer.position(0)

        then:
        fc.read(buffer) == fileSize

        when:
        buffer.position(0)

        then:
        for (int i = 0; i < DataNodeServer.BLOCK_SIZE - 3; i++)
            buffer.get() == 0.byteValue()
        buffer.get() == 0.byteValue()
        buffer.get() == 1.byteValue()
        buffer.get() == 2.byteValue()
        buffer.get() == 3.byteValue()
        buffer.get() == 4.byteValue()

        when:
        fc.truncate(fileSize)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == fileSize

        when:
        fc.close()
        fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileNode.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer

        cleanup:
        fc.close()
    }

    private static String generateFilename() {
        RandomStringUtils.random(255).replace('/', ':')
    }

    private static int getBlockAmount(int fileSize) {
        fileSize == 0 ? 0 : (((fileSize - 1) / DataNodeServer.BLOCK_SIZE) + 1) as int
    }
}