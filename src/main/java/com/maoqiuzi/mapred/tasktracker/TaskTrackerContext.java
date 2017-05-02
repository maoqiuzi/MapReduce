package com.maoqiuzi.mapred.tasktracker;

import com.maoqiuzi.mapred.common.ContextInterface;
import com.maoqiuzi.mapred.common.Directory;
import com.maoqiuzi.mapred.jobtracker.InputSplit;

import java.io.*;
import java.util.List;
import java.util.Map;

//import common.ContextInterface;
//import common.Directory;
//import common.FtpServerMR;
//import jobtracker.InputSplit;

public class TaskTrackerContext implements ContextInterface {
    private String ip;
    private int port;
    private int ftpServerPort;
    private String jobId;
    private String taskTrackerId;
    private Directory directory;
    private SlaveStatus status;
    // for tasks
    private TaskType taskType;
    // input path is the overall input path
    private String inputPath;
    // the overall output path
    private String outPath;
    private String jarFileName;
    //for mapper assignment
    private String mapperClassName;
    private InputSplit inputSplit;
    //output filename, for reducer to upload
    private String reducerOutputFilename;
    //for mapper return
    /*
     * the first string is the partition of the result
	 * the second string is the filename of the result
	 * the reducer will use this filename to retrieve
	 * the result from ftp server in this machine
	 */
    private List<ReducerAssignment> mapperResult;
    //for reducer assignment
    private String reducerClassName;
    // for write
    //private PrintWriter printWriter;
    private Map<String,List<ReducerAssignment>> reducerAssignments;
    //for reducer return
    private List<ReducerAssignment> reducerResult;

    public String getTaskTrackerId() {
        return taskTrackerId;
    }

    public void setTaskTrackerId(String taskTrackerId) {
        this.taskTrackerId = taskTrackerId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
        this.directory = new Directory(this.getTaskTrackerId(),jobId);
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getFtpServerPort() {
        return ftpServerPort;
    }

    public void setFtpServerPort(int ftpServerPort) {
        this.ftpServerPort = ftpServerPort;
    }

    public SlaveStatus getStatus() {
        return status;
    }

    public void setStatus(SlaveStatus status) {
        this.status = status;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {

        return outPath;
    }

    public void setOutputPath(String outPath) {
        this.outPath = outPath;
    }

    public String getJarFileName() {
        return jarFileName;
    }

    public void setJarFileName(String jarFileName) {
        this.jarFileName = jarFileName;
    }

    public String getMapperClassName() {
        return mapperClassName;
    }

    public void setMapperClassName(String mapperClassName) {
        this.mapperClassName = mapperClassName;
    }


    public InputSplit getInputSplit() {
        return inputSplit;
    }

    public void setInputSplit(InputSplit inputSplit) {
        this.inputSplit = inputSplit;
    }

    public List<ReducerAssignment> getMapperResult() {
        return mapperResult;
    }

    public void setMapperResult(List<ReducerAssignment> mapperResult) {
        this.mapperResult = mapperResult;
    }

    public String getReducerClassName() {
        return reducerClassName;
    }

    public void setReducerClassName(String reducerClassName) {
        this.reducerClassName = reducerClassName;
    }

    public Map<String,List<ReducerAssignment>> getReducerAssignments() {
        return reducerAssignments;
    }

    public void setReducerAssignments(Map<String,List<ReducerAssignment>> reducerAssignments) {
        this.reducerAssignments = reducerAssignments;
    }

    public List<ReducerAssignment> getReducerResult() {
        return reducerResult;
    }

    public void setReducerResult(List<ReducerAssignment> reducerResultFilename) {
        this.reducerResult = reducerResultFilename;
    }

    public void print() {
        System.out.println("ip:" + this.ip);
        //System.out.println("port"+this.port);
        System.out.println("ftpServerPort" + this.ftpServerPort);
    }

    public String getReducerOutputFilename() {
        return reducerOutputFilename;
    }

    public void setReducerOutputFilename(String reducerOutputFilename) {
        this.reducerOutputFilename = reducerOutputFilename;
    }

    /**
     * K and V should have toString()
     *
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     */
    public <K, V> void write(K key, V value) throws FileNotFoundException {
        if (taskType == TaskType.MAPPER) {
            mapperWrite(key, value);
        } else {
            ReducerWrite(key, value);
        }
    }

    private <K, V> void ReducerWrite(K key, V value) {
        PrintWriter printWriter = initPrinterWriter(this.directory.getReducerOutputDirectory()+"/"+key.toString());
        if (printWriter != null) {
            printWriter.println(key.toString() + " " + value.toString());
        }
        printWriter.close();
    }

    /**
     * write mapper result to file, and if there is new partition,
     * add the partition and filename into mapper result
     *
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     * @throws FileNotFoundException
     */
    public <K, V> void mapperWrite(K key, V value) throws FileNotFoundException {
        PrintWriter printWriter = initPrinterWriter(this.directory.getMapperOutputDirectory()+"/"+key.toString());
        if (printWriter != null) {
            //printWriter.println(key.toString() + " " + value.toString());
            printWriter.println(value.toString());
        }
        printWriter.close();
        addKeyToMapperResult(key.toString());
    }

    private void addKeyToMapperResult(String key) {
        if (!mapperResultHasKey(key)) {
            System.out.println(key);
            ReducerAssignment assignment = new ReducerAssignment();
            assignment.setFilePath(directory.getMapperOutputDirectory() + "/" + key);
            assignment.setPartition(key);
            assignment.setServerIp(ip);
            assignment.setServerPort(ftpServerPort);
            mapperResult.add(assignment);
        }
    }

    private boolean mapperResultHasKey(String key) {
        for (int i = 0; i < mapperResult.size(); i++) {
            if (mapperResult.get(i).getPartition().equals(key)) {
                return true;
            }
        }
        return false;
    }

    public PrintWriter initPrinterWriter(String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            return new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
        } catch (FileNotFoundException e) {
            System.err.println("Reduce result file couldn't be found");
            e.printStackTrace();
            return null;
        } catch (IOException io) {
            System.err.println("Reduce result couldn't be create");
            io.printStackTrace();
            return null;
        }
    }

    public void reset() {
        this.status = SlaveStatus.IDLE;
        inputPath = "";
        inputSplit = null;
        jarFileName = "";
        mapperClassName = "";
        mapperResult = null;
        outPath = "";
        reducerAssignments = null;
        reducerClassName = "";
        reducerOutputFilename = "";
        reducerResult = null;
        taskType = TaskType.UNSIGNED;
    }
}
