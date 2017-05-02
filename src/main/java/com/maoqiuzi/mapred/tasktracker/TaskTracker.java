package com.maoqiuzi.mapred.tasktracker;


//import common.Directory;
//import common.FtpServerMR;
//import common.Hdfs;
//import common.NetworkUtils;

import com.maoqiuzi.mapred.common.Directory;
import com.maoqiuzi.mapred.common.FtpServerMR;
import com.maoqiuzi.mapred.common.Hdfs;
import com.maoqiuzi.mapred.common.NetworkUtils;

import java.io.*;
import java.net.*;
import java.util.*;

public class TaskTracker implements Runnable {
    private FtpServerMR ftpServer;
    private Socket jobTrackerSocket;
    private String jobTrackerIp;
    private int jobTrackerPort;
    private String taskTrackerId;
    private int ftpport;
    private Hdfs hdfs;
    private TaskTrackerContext context;
    private Directory directory;

    /**
     * initialization
     */
    public TaskTracker() {
        hdfs = new Hdfs();
        this.jobTrackerIp = hdfs.getMasterIp();
        this.jobTrackerPort = hdfs.getMasterPort();
        ftpport = randomSparePort();
        this.taskTrackerId = Integer.toString(this.ftpport);
        this.directory = new Directory(this.taskTrackerId);
        context = new TaskTrackerContext();
        context.setTaskTrackerId(this.taskTrackerId);
        context.setStatus(SlaveStatus.IDLE);
        ftpServer = new FtpServerMR(ftpport, taskTrackerId);
    }

    public static void main(String[] args) {
        TaskTracker taskTracker = new TaskTracker();
        taskTracker.run();
    }

    /**
     * first, start a FTPServer, and then tries to connect jobtracker. the
     * IP address and FtpServerPort is on cloud(via hdfs can get that).
     * once connected to jobtracker, it will enter waitforassignment phase
     */
    public void run() {
        System.out.println("Starting a FTP server");
        startFtpServer();
        System.out.println("Connecting to JobTracker");
        connectToJobTracker();
        System.out.println("Connected to JobTracker");
        initContext();
        System.out.println("Sending context to JobTracker");
        NetworkUtils.sendContext(this.jobTrackerSocket, this.context);
        System.out.println("Finish sending.");
        waitForAssignment();
    }

    //<editor-fold desc="startFtpServer">
    private void startFtpServer() {
        this.ftpServer = new FtpServerMR(ftpport, taskTrackerId);
        this.ftpServer.run();
        System.out.printf("Ftp server on host self, port: %d", ftpport);
        System.out.println();
    }

    private int randomSparePort() {
        Random rand = new Random();
        int result;
        do {
            result = rand.nextInt(65535 - 1024 + 1) + 1024;
        } while (result == this.jobTrackerPort);
        return result;
    }

    //</editor-fold>
    //<editor-fold desc="connectToJobTracker">
    private void connectToJobTracker() {
        try {
            System.out.println("jobTrackIp:" + jobTrackerIp);
            System.out.println("jobTrackerPort:" + jobTrackerPort);
            this.jobTrackerSocket = new Socket(InetAddress.getByName(jobTrackerIp), jobTrackerPort);
        } catch (IOException ioe) {
            System.err.println("Fail to connect JobTracker");
            ioe.printStackTrace();
        }
    }

    //</editor-fold>
    //<editor-fold desc="initContent">

    /**
     * pack the initial information of this TaskTracker into TaskTrackerContext
     * should at least include Ip, Port, ftpPort
     * For JobTracker's convenience, set status and task type to default
     *
     * @return the context
     */
    private void initContext() {

        this.context.setIp(this.getLocalPublicIp());
        this.context.setPort(this.getLocalPort());
        this.context.setFtpServerPort(this.ftpServer.getPort());
        this.context.setStatus(SlaveStatus.IDLE);
        this.context.setTaskType(TaskType.UNSIGNED);
    }

    /**
     * @return local IP address
     */
    private String getLocalPublicIp() {
        String ip = null;
        try {
            URL whatismyip = new URL("http://checkip.amazonaws.com");
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    whatismyip.openStream()));

            ip = in.readLine();
            //ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ip;
    }

    private int getLocalPort() {
        return this.jobTrackerSocket.getLocalPort();
    }
    //</editor-fold>

    //<editor-fold desc="waitForAssignment">

    /**
     * a while loop tries to receive TaskTrackerContext, once received
     * a TaskTrackerContext, read the type of the task. to accordingly
     * start a mapper or reducer.
     */
    private void waitForAssignment() {
        while (true) {
            System.out.println("Waiting for assignment");
            context = (TaskTrackerContext) NetworkUtils.recvContext(jobTrackerSocket);
            directory.initJobRootDirectory(context.getJobId());
            System.out.println("Context received");
            System.out.println("JarFileName: " + context.getJarFileName());
            fetchJarFile(context.getJarFileName());
            System.out.println("JarFile Fetched");
            if (context.getTaskType() == TaskType.MAPPER) {
                context.setStatus(SlaveStatus.MAPPING);
                System.out.println("Started Mapping");
                sendResponse();
                System.out.println("response Sent");
                map();
            } else if (context.getTaskType() == TaskType.REDUCER) {
                context.setStatus(SlaveStatus.REDUCING);
                System.out.println("Started Reducing");
                sendResponse();
                System.out.println("response Sent");
                reduce();
            }
            assignmentComplete();
            context.setStatus(SlaveStatus.IDLE);
        }
    }

    //<editor-fold desc="fetchJarFile">

    /**
     * fetch the jar file from ftp server and cache it locally for future use
     *
     * @param jarFileName
     */
    private void fetchJarFile(String jarFileName) {
        if (jarFileExists(jarFileName)) {
            return;
        }
        hdfs.downloadS3(jarFileName, this.directory.getJobJarFileDirectory() + "/" + jarFileName);
    }

    private boolean jarFileExists(String jarFileName) {
        File jarFile = new File(this.directory.getJobJarFileDirectory() + "/" + jarFileName);
        return jarFile.exists();
    }
    //</editor-fold>
    //<editor-fold desc="sendResponse">

    /**
     * send a response back to JobTracker saying the status of self
     */
    public void sendResponse() {
        if (!NetworkUtils.sendContext(this.jobTrackerSocket, this.context)) {
            System.err.println("fail to send map/reduce running context");
        }

    }

    //</editor-fold>

    //<editor-fold desc="map reduce">
    private void map() {
        System.out.println("mapping...");
        Mapper mapper = this.loadMapperClass();
        System.out.println("Mapper class: " + mapper.toString());
        //set context mapperResult in method TaskTrackerContext.writeMap()
        // save map result files in FtpServerMR.mapDirectory,file name format "{ip}-{key}-map"
        SlaveStatus result = mapper.run(context);
        context.setStatus(result);

    }

    private void reduce() {
        System.out.println("reducing...");
        Reducer reducer = this.loadReducerClass();
        SlaveStatus result = reducer.run(context);
        context.setStatus(result);
    }


    private Mapper loadMapperClass() {
        return loadClass(Mapper.class, context.getMapperClassName(), this.context.getJarFileName());
    }

    private Reducer loadReducerClass() {
        return loadClass(Reducer.class, context.getReducerClassName(), this.context.getJarFileName());
    }

    /**
     * load a T class from jar into an object.
     *
     * @param cls
     * @param className
     * @param jarFileName
     * @param <T>
     * @return
     */
    private <T> T loadClass(Class<T> cls, String className, String jarFileName) {
        try {
            URL url = new File(this.directory.getJobJarFileDirectory() + "/" + jarFileName).toURI().toURL();
            System.out.println("url: " + url.toString());
            URLClassLoader loader = new URLClassLoader(new URL[]{url});
            System.out.println("starting load class: " + className);
            Class c = loader.loadClass(className);
            return (T) c.newInstance();
        } catch (MalformedURLException mue) {
            System.err.printf("Exception occurs when generate %s url", className);
            mue.printStackTrace();
            return null;
        } catch (ClassNotFoundException cnfe) {
            System.err.printf("Exception occurs when load %s class", className);
            cnfe.printStackTrace();
            return null;
        } catch (Exception iae) {
            System.err.printf("Exception occurs when create %s Instance", className);
            iae.printStackTrace();
            return null;
        }
    }


    //</editor-fold>

    //<editor-fold desc="assignmentComplete">

    /**
     * once the assignment completed, generate a TaskTrackerContext,
     * and send the context back to JobTracker.Then go to waitForAssignment
     * for another assignment
     */
    public void assignmentComplete() {
        System.out.println("Assignment completed, Sending back context.");
        if (!NetworkUtils.sendContext(this.jobTrackerSocket, this.context)) {
            System.err.println("fail to send map/reduce result context");
        }
    }
    //</editor-fold>


    //</editor-fold>

}
