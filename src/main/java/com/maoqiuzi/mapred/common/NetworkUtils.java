package com.maoqiuzi.mapred.common;

//import com.maoqiuzi.mapred.job.JobContext;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by wangjinpeng on 4/14/15.
 */
public class NetworkUtils {

    /**
     * send a context via given socket
     * @param socket
     * @param contextInterface
     * @return sent succeeded or not
     */
    public static boolean sendContext(Socket socket, ContextInterface contextInterface){
        boolean succeed = false;
        try {
            ObjectOutputStream stream = new ObjectOutputStream(socket.getOutputStream());
            stream.writeObject(contextInterface);
            succeed = true;
        } catch (IOException e) {
            System.err.println("Exception occur when send JobContext.");
            e.printStackTrace();
            succeed = false;
        }
        return succeed;
    }

    public static ContextInterface recvContext(Socket socket){
        ContextInterface contextInterface = null;
        try {
            ObjectInputStream stream = new ObjectInputStream(socket.getInputStream());
            contextInterface = (ContextInterface) stream.readObject();
        } catch (Exception ex) {
            System.out.println("Cannot get context.");
            ex.printStackTrace();
        }
        return contextInterface;
    }

}
