package sdfs.client;

import javafx.beans.binding.ObjectExpression;
import sdfs.namenode.SDFSFileChannel;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SdfsRmi {

    public static Object send(InetSocketAddress inetSocketAddress, String method, Class<?>[] paraType, Object[] paras) throws IOException {
        Socket socket = new Socket(inetSocketAddress.getHostName(), inetSocketAddress.getPort());
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(method);
        out.writeObject(paraType);
        out.writeObject(paras);


        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
        Object object = null;
        try {
            object = in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        socket.close();
        return object;
    }


}
