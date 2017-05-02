package com.maoqiuzi.mapred.common;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.IOUtils;
import com.sun.xml.internal.bind.v2.TODO;

/*
 * responsible for read input and write output, maybe on aws s3 or locally
 */
public class Hdfs {

    private AmazonS3 s3;
    private static String bucketName = "hadoopmultinodetest";

    private static String masterInfoFilename = "masterInfo";

    public Hdfs() {
        AWSCredentials qiudicredentials = null;
        try {
             qiudicredentials = new BasicAWSCredentials("xxx", "xxx");
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. "
                            + "Please make sure that your credentials file is at the correct "
                            + "location (/root/.aws/credentials), and is in valid format.",
                    e);
        }
        this.s3 = new AmazonS3Client(qiudicredentials);
    }

    /**
     * load the data from destination from given path from startOffset to
     * endOffset, maybe on s3. If the endOffset is larger than the actual offset,
     * just return the remaining characters.
     */
    public InputStream readS3(String filename, long startOffset, long endOffset) throws IOException {
        System.out.println("Reading from S3, reading file: " + filename);
        GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, filename);
        rangeObjectRequest.setRange(startOffset, endOffset);
        S3Object objectPortion = s3.getObject(rangeObjectRequest);

        InputStream input = objectPortion.getObjectContent();

        return input;
    }

    public Long getFileSizeS3(String filename) {
        ObjectMetadata data = s3.getObjectMetadata(bucketName, filename);
        Long size = data.getInstanceLength();
        return size;
    }


    /**
     * write a input string into the given filename on s3
     * @param filename the destination filename
     * @param input    a input string to write to the destination file
     */
    public void writeS3(String filename, String input) {
        byte[] contentToBytes = input.getBytes(Charset.forName("UTF-8"));
        ByteArrayInputStream inputStream = new ByteArrayInputStream(contentToBytes);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentToBytes.length);
        try {
            PutObjectResult por = s3.putObject(bucketName, filename, inputStream, metadata);
            inputStream.close();
        } catch (AmazonServiceException ase) {
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Error Message: " + ace.getMessage());
        } catch (IOException e) {
            System.out.print(e);
        }
    }

    /**
     * upload file to destination, maybe on s3, make sure the src is
     * unique(maybe by timestamp). Notice, this will override any existing
     * dest file on s3
     *
     * @param src  the file to be uploaded
     * @param dest the destination filename
     */
    public void uploadS3(String src, String dest) {
        TransferManager tm = new TransferManager(this.s3);
        File file = new File(src);
        Upload upload = tm.upload(bucketName, dest, file);
        try {
            upload.waitForCompletion();
            System.out.println("Upload complete.");
        } catch (AmazonClientException amazonClientException) {
            System.out.println("Unable to upload file, upload was aborted.");
            amazonClientException.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("Unable to upload file, upload Interrupted.");
            e.printStackTrace();
        }
    }

    /**
     * download a file form src on s3 to dest on local. if dest file exists,
     * local dest file will be overwritten
     * @param src
     * @param dest
     */
    public void downloadS3(String src, String dest) {
        TransferManager tm = new TransferManager(this.s3);
        File file = new File(dest);
        Download download = tm.download(bucketName, src, file);
        try {
            download.waitForCompletion();
        } catch (InterruptedException e) {
            System.out.println("Unable to download file, download Interrupted.");
            e.printStackTrace();
        }
    }

    /**
     * remove a file on s3
     *
     * @param filename
     */
    public void removeS3(String filename) {
        try {
            s3.deleteObject(new DeleteObjectRequest(bucketName, filename));
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    /**
     * this utility function is used to read mapper results from other machines.
     */
    public void readFrom(String hostIp, int hostPort, String pathtofile) {

    }

    /**
     * get all the filenames in a directory
     * @param directory
     * @return
     */
    public List<String> getFilesInDirectory(String directory){
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(directory + "/");

        ObjectListing objectListing = s3.listObjects(listObjectsRequest);
        List<String> results = new LinkedList<String>();
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            String key = summary.getKey();
            if(!key.endsWith("/")) {
                String[] keys = key.split("/");
                key = keys[keys.length-1];
                results.add(key);
            }
        }
        return results;
    }


    /**
     * these 3 function is the central location for all slaves and master to
     * store the master ip and port, thus can dynamically decide the master ip
     * and port
     */
    public void setMasterIpPort(String ip, int port) {
        String lines = "The master ip address is:\n";
        lines += ip + "\n";
        lines += "The master port is:\n";
        lines += String.valueOf(port);

        // convert the lines into inputstream
        //InputStream stream = new ByteArrayInputStream(lines.getBytes(StandardCharsets.UTF_8));
        writeS3(masterInfoFilename, lines);
    }


    /**
     * the ip address of master is stored on aws s3
     *
     * @return the ip address of master
     */
    public String getMasterIp() {
        InputStream input = null;
        String ip = null;
        try {
            input = readS3("masterInfo", 0, 100);
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            reader.readLine();
            ip = reader.readLine();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ip;
    }


    /**
     * the port info of master is stored on aws s3
     *
     * @return the port of master
     */
    public int getMasterPort() {
        InputStream input = null;
        int port = 0;
        try {
            input = readS3("masterInfo", 0, 100);
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            reader.readLine();
            reader.readLine();
            reader.readLine();
            port = Integer.parseInt(reader.readLine());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return port;
    }

}
