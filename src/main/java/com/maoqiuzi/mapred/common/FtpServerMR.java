package com.maoqiuzi.mapred.common;

import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.*;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.TransferRatePermission;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.ftpserver.FtpServer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * First, start a ftp server
 * On initialization, should accept a path, on which to hold as root directory
 * you should be responsible to make a directory if the path not exist.
 * <p>
 * then, provide 1 functions, getFtpPort() to return the current port
 */
public class FtpServerMR {

    private FtpServerFactory serverFactory;
    private ListenerFactory listenerFactory;
    private PropertiesUserManagerFactory userManagerFactory;
    private int port;

    private String userPropertyFile;

    private Directory directory;

    /**
     * initialize directory and setup a ftp server
     * initialize the directory of tasktracker, the directory will contain a user
     * directory.
     *
     * @param ftpserverPort
     * @param taskTrackerId
     */
    public FtpServerMR(int ftpserverPort, String taskTrackerId) {
        directory = new Directory(taskTrackerId);
        this.userPropertyFile = this.directory.getUserDirectory() + "/users.properties";
        this.serverFactory = new FtpServerFactory();
        this.initListener(ftpserverPort);
        this.initUserManagerProperty();
        this.initFtplets();
    }

    private void initListener(int ftpserverPort) {
        this.port = ftpserverPort;
        this.listenerFactory = new ListenerFactory();
        listenerFactory.setPort(ftpserverPort);
        serverFactory.addListener("default", listenerFactory.createListener());
    }

    //set user factory property
    private void initUserManagerProperty() {
        this.initUserFile();
        this.userManagerFactory = new PropertiesUserManagerFactory();
        File userPropertyFile = new File(this.userPropertyFile);
        this.userManagerFactory.setFile(userPropertyFile);
        this.setEncryptor();
        this.initUser();
    }

    private void initUserFile() {
        File userProFile = new File(this.userPropertyFile);
        try {
            userProFile.createNewFile();
        } catch (IOException ioe) {
            System.err.println("Couldn't create user property file: " + this.userPropertyFile);
        }
    }

    private void setEncryptor() {
        this.userManagerFactory.setPasswordEncryptor(new PasswordEncryptor() {
            public String encrypt(String password) {
                return password;
            }

            public boolean matches(String passwordToCheck, String storedPassword) {
                return passwordToCheck.equals(storedPassword);
            }
        });
    }

    private void initUser() {
        BaseUser user = new BaseUser();
        user.setName("test");
        user.setPassword("test");
        user.setHomeDirectory(this.directory.getTaskTrackerRootDirectory());
        List<Authority> authorities = new ArrayList<Authority>();
        authorities.add(new WritePermission());
        // set the transmission rate, in Bps
        int rate100m = 100000000;
        authorities.add(new TransferRatePermission(rate100m, rate100m));

        user.setAuthorities(authorities);
        UserManager um = userManagerFactory.createUserManager();
        try {
            um.save(user);//Save the user to the user list on the filesystem
        } catch (FtpException fe) {
            fe.printStackTrace();
            System.out.println("Couldn't save user in UserManager");
        }
        serverFactory.setUserManager(um);
    }

    private void initFtplets() {
        Map<String, Ftplet> m = new HashMap<String, Ftplet>();
        m.put("miaFtplet", new Ftplet() {
            public void init(FtpletContext ftpletContext) throws FtpException {
                System.out.println("init");
                System.out.println("Thread #" + Thread.currentThread().getId());
            }

            public void destroy() {
                System.out.println("destroy");
                System.out.println("Thread #" + Thread.currentThread().getId());
            }

            public FtpletResult beforeCommand(FtpSession session, FtpRequest request) throws FtpException, IOException {
                System.out.println("beforeCommand " + session.getUserArgument() + " : " + session.toString() + " | " + request.getArgument() + " : " + request.getCommand() + " : " + request.getRequestLine());
                System.out.println("Thread #" + Thread.currentThread().getId());

                //do something
                return FtpletResult.DEFAULT;//...or return accordingly
            }

            public FtpletResult afterCommand(FtpSession session, FtpRequest request, FtpReply reply) throws FtpException, IOException {
                System.out.println("afterCommand " + session.getUserArgument() + " : " + session.toString() + " | " + request.getArgument() + " : " + request.getCommand() + " : " + request.getRequestLine() + " | " + reply.getMessage() + " : " + reply.toString());
                System.out.println("Thread #" + Thread.currentThread().getId());

                //do something
                return FtpletResult.DEFAULT;//...or return accordingly
            }

            public FtpletResult onConnect(FtpSession session) throws FtpException, IOException {
                System.out.println("onConnect " + session.getUserArgument() + " : " + session.toString());
                System.out.println("Thread #" + Thread.currentThread().getId());

                //do something
                return FtpletResult.DEFAULT;//...or return accordingly
            }

            public FtpletResult onDisconnect(FtpSession session) throws FtpException, IOException {
                System.out.println("onDisconnect " + session.getUserArgument() + " : " + session.toString());
                System.out.println("Thread #" + Thread.currentThread().getId());

                //do something
                return FtpletResult.DEFAULT;//...or return accordingly
            }
        });
        serverFactory.setFtplets(m);
    }


    /**
     * (non-Javadoc)
     *
     * @see Runnable#run()
     * this is used for other machine visit files on this machine
     * the run method accept socket connection and servers transfering
     * files between them
     */
    public void run() {
        FtpServer server = serverFactory.createServer();
        try {
            server.start();
        } catch (FtpException ex) {
            System.out.println("Couldn't start ftp server");
        }
    }

    //for testing
    public static void main(String[] args) {
        FtpServerMR ftpServerMR = new FtpServerMR(6000, "1233");
        ftpServerMR.run();
    }

    public int getPort() {
        return port;
    }
}
