package com.maoqiuzi.mapred.job;


import com.maoqiuzi.mapred.common.NetworkUtils;
import com.maoqiuzi.mapred.common.Hdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

public class Job {
    private static Job instance = null;
    private JobContext context;
    private Socket socket;

    public void setUploadJar(boolean uploadJar) {
        this.uploadJar = uploadJar;
    }

    private boolean uploadJar = true;

    private Job() {
        this.context = new JobContext();
    }

    /**
     * pass configuration to JobContext
     * set it as an argument in JobContext
     */
    public static Job getInstance(Configuration conf) {
        if(instance == null) {
            instance = new Job();
        }
        instance.context.setConf(conf);
        return instance;
    }



    /**
     * set the mapper class to be executed, store the class name to send to Master server
     */
    public void setMapperClass(Class cls) {
        this.context.setMapper(cls.getName());
    }

    /**
     * set the reducer class to be executed, store the class name to send to Master server
     */
    public void setReducerClass(Class cls) {
        this.context.setReducer(cls.getName());
    }

    /**
     * set the input path, store the path and send it to Master server
     * make sure the input path is accessible
     */
    public void addInputPath(String path) {
        this.context.setInputPath(path);
    }

    /**
     * set output path, store the path and send it to Master server
     */
    public void setOutputPath(String path) {
        this.context.setOutputPath(path);
    }

    /**
     * actually run the program, save path, class names into context,
     * and send the JobContext to Master(JobTracker), then loop for
     * success or failure.
     * once completed, the jobtracker will send back a JobContext, indicating success or failure
     */
    public void setJarFileName(String jarFileName)
    {
        this.context.setJarFileName(jarFileName);
    }

    public void waitForCompletion() {
        System.out.println("Sending Jar file to S3");
        if (uploadJar == true) {
            sendJarfileToS3();
        }
        System.out.println("Connecting to JobTracker");
        connectToJobTracker();
        System.out.println("Sending Context to JobTracker");
        sendContextToJobTracker();
        this.context = waitForResult();
        if (this.context.isSucceed()) {
            System.out.println("job completed");
        } else {
            System.out.println("job failed");
        }
    }

    private void sendJarfileToS3() {
        Hdfs hdfs = new Hdfs();
        String exePath = Job.class.getProtectionDomain().getCodeSource().getLocation().getPath().toString();
        String filename = this.context.getJarFileName();
        hdfs.uploadS3(exePath, filename);
    }

    /**
     * connect to JobTracker
     * Firstly get Master IP address and port using HDFS
     * Then use socket to connect to it
     */
    private void connectToJobTracker() {
        Hdfs hdfs = new Hdfs();
        try {
            String jobTrackerIp = hdfs.getMasterIp();
            int jobTrackerPort = hdfs.getMasterPort();
            System.out.println("Connecting to:" + jobTrackerIp);
            this.socket = new Socket(InetAddress.getByName(jobTrackerIp), jobTrackerPort);

            System.out.println("Connected.");
        } catch (IOException ex) {
            System.out.println("Can not connect to job tracker.");
            ex.printStackTrace();
        }
    }

    /**
     * send JobContext to job using socket passed in
     * use try and catch to handle exception
     * use while loop to rerend context if it fails
     */
    private void sendContextToJobTracker() {
        boolean succeed = NetworkUtils.sendContext(this.socket, this.context);
    }

    /**
     * collect data from stream
     * combine them and return it
     * @return JobContext
     */
    private JobContext waitForResult() {
        JobContext context = (JobContext) NetworkUtils.recvContext(this.socket);
        return context;
    }
}
