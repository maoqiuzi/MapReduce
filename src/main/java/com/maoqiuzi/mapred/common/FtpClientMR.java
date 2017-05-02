package com.maoqiuzi.mapred.common;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;

/**
 * Created by Qiudi on 2015/4/14.
 * Download file from FtpServerMR, should provide a function
 * static download(remoteIp, remotePort, filename)
 */
public class FtpClientMR {
    private static String userId = "test";
    private static String password = "test";

    /**
     *
     * @param remoteIp
     * @param remotePort
     * @param tackTrackerRootDirectory is the path to the source file to download
     * @return
     * @throws IOException
     */
    private static FTPClient initConnection(String remoteIp, int remotePort, String tackTrackerRootDirectory) throws IOException {
        FTPClient ftp = new FTPClient();
        ftp.setDefaultPort(remotePort);
        try {
            ftp.connect(remoteIp);
        } catch (SocketException se) {
            System.out.println("Couldn't connect to ftp server");
            se.printStackTrace();
        }
        if (!ftp.login(userId, password)) {
            ftp.logout();
            throw new IOException("Couldn't login FTPServer");
        }
        int reply = ftp.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            ftp.disconnect();
            throw new IOException("FTPServer reply negative");
        }
        //enter passive mode
        ftp.enterLocalPassiveMode();
        //change current directory

        System.out.println("Current working directory is : " + tackTrackerRootDirectory + "/");
        //ftp.changeWorkingDirectory(tackTrackerRootDirectory);
        System.out.println("Current directory is : " + ftp.printWorkingDirectory());
        //get system name
        System.out.println("Remote system is " + ftp.getSystemType());
        return ftp;
    }

    private static void closeConnection(FTPClient ftp) throws IOException {
        ftp.logout();
        ftp.disconnect();
    }

    public static void download(String remoteIp, int remotePort,
                               String srcFilePath, String destFilePath) throws IOException {

        System.out.printf("connecting to ftp server at IP: %s, Port: %d", remoteIp, remotePort);
        System.out.println();
        String srcFileDirectory = srcFilePath.substring(0, srcFilePath.lastIndexOf('/'));
        String srcFileName = srcFilePath.substring(srcFilePath.lastIndexOf('/')+1,srcFilePath.length());
        System.out.println("Download srcFileName is : " + srcFileName);
        FTPClient ftp = initConnection(remoteIp, remotePort, srcFileDirectory);
        //start download file
        OutputStream output = new FileOutputStream(destFilePath);
        String srcRelativeFilePath = srcFilePath.substring(srcFilePath.indexOf("Job-"));
        boolean success = ftp.retrieveFile(srcRelativeFilePath, output);
        output.close();
        if (success) {
            System.out.println("File " + srcFileName + " downloaded successfully");
        } else {
            System.out.println("File " + srcFileName + " downloaded failed");
        }

        closeConnection(ftp);
    }

    //if directory doesn't exist create it
    private static boolean createDir(File file) {
        if (!file.exists()) {
            return file.mkdir();
        }
        return true;
    }

    //if file doesn't exist create it
    private static void createFile(File file) throws IOException {
        if (!file.exists()) {
            file.createNewFile();
        }
        return;
    }
}
