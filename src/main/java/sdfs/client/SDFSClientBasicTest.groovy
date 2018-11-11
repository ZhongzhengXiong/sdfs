/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client

import org.apache.commons.lang3.RandomStringUtils
import sdfs.datanode.DataNodeServer
import sdfs.exception.SDFSFileAlreadyExistException
import sdfs.namenode.NameNodeServer
import sdfs.protocol.INameNodeDataNodeProtocol
import sdfs.protocol.INameNodeProtocol
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import java.rmi.registry.LocateRegistry
import java.rmi.registry.Registry
import java.rmi.server.UnicastRemoteObject

class SDFSClientBasicTest extends Specification {
    @Shared
    def buffer = ByteBuffer.allocate(1)
    @Shared
    SDFSClient client = new SDFSClient(new InetSocketAddress("localhost", 4343), 16 )
    @Shared
    NameNodeServer nameNodeServer
    @Shared
    DataNodeServer dataNodeServer

    def setupSpec() {
        System.setProperty("user.dir", File.createTempDir().absolutePath);
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

    def "Test file tree"() {
        def parentDir = generateFilename()
        client.mkdir(parentDir)
        for (int i = 0; i < 255; i++)
            client.mkdir(parentDir += "/" + generateFilename())
        def dirName = generateFilename()
        def filename = generateFilename()
        client.mkdir("$parentDir/$dirName")
        client.create("$parentDir/$filename").close()

        when:
        client.mkdir("$parentDir/$dirName")

        then:
        thrown SDFSFileAlreadyExistException

        when:
        client.mkdir("$parentDir/$filename")

        then:
        thrown SDFSFileAlreadyExistException

        when:
        client.create("$parentDir/$dirName")

        then:
        thrown SDFSFileAlreadyExistException

        when:
        client.create("$parentDir/$filename")

        then:
        thrown SDFSFileAlreadyExistException

        when:
        client.openReadonly("$parentDir/${generateFilename()}")

        then:
        thrown FileNotFoundException

        when:
        client.openReadWrite("$parentDir/${generateFilename()}")

        then:
        thrown FileNotFoundException

        when:
        client.openReadonly("${generateFilename()}/$filename")

        then:
        thrown FileNotFoundException

        when:
        client.openReadWrite("${generateFilename()}/$filename")

        then:
        thrown FileNotFoundException

        when:
        client.create("${generateFilename()}/$filename")

        then:
        thrown FileNotFoundException
    }

    def "Test create empty"() {
        def parentDir = generateFilename()
        client.mkdir(parentDir)
        def filename = parentDir + "/" + generateFilename()

        when:
        def fc = client.create(filename)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 0
        fc.read(buffer) == 0

        when:
        fc.position(-1)

        then:
        thrown IllegalArgumentException

        when:
        fc.position(1)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 1
        fc.read(buffer) == 0

        when:
        fc.close()

        then:
        !fc.isOpen()

        when:
        fc.position()

        then:
        thrown ClosedChannelException

        when:
        fc.position(0)

        then:
        thrown ClosedChannelException

        when:
        fc.read(buffer)

        then:
        thrown ClosedChannelException

        when:
        fc.write(buffer)

        then:
        thrown ClosedChannelException

        when:
        fc.size()

        then:
        thrown ClosedChannelException

        when:
        fc.flush()

        then:
        thrown ClosedChannelException

        when:
        fc.truncate(0)

        then:
        thrown ClosedChannelException

        when:
        fc = client.openReadonly(filename)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 0

        when:
        fc.write(buffer)

        then:
        thrown NonWritableChannelException

        when:
        fc.truncate(0)

        then:
        thrown NonWritableChannelException
    }

    private static String generateFilename() {
        RandomStringUtils.random(255).replace('/', ':')
    }
}