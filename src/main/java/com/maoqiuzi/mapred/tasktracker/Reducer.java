package com.maoqiuzi.mapred.tasktracker;
//
//import common.Directory;
//import common.FtpClientMR;
//import common.FtpServerMR;
//import common.Hdfs;

import com.maoqiuzi.mapred.common.Directory;
import com.maoqiuzi.mapred.common.FtpClientMR;
import com.maoqiuzi.mapred.common.Hdfs;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * K1 and V1 should be String and String for this version of mapreduce
 *
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public abstract class Reducer<K1, V1, K2, V2> {

    private Directory directory;


    public abstract void reduce(K1 keyin, Iterator<V1> values, TaskTrackerContext context);

    public SlaveStatus run(TaskTrackerContext context) {
        this.directory = new Directory(context.getTaskTrackerId(),context.getJobId());
        Map<K1, List<ReducerAssignment>> partitionAssignments = (Map<K1, List<ReducerAssignment>>) context.getReducerAssignments();
        Iterator<K1> partitionNames = partitionAssignments.keySet().iterator();
        while (partitionNames.hasNext()) {
            K1 partitionName = partitionNames.next();
            List<ReducerAssignment> assignments = partitionAssignments.get(partitionName);
            try {
                reduceOnePartition(partitionName, assignments, context);
            } catch (IOException e) {
                e.printStackTrace();
                return SlaveStatus.REDUCEFAILED;
            }
        }
        return SlaveStatus.REDUCESUCCEEDED;
    }

    private void reduceOnePartition(K1 partitionName, List<ReducerAssignment> assignments, TaskTrackerContext context) throws IOException {
        List<V1> values = null;
        values = (List<V1>) shuffle(assignments);
        context.setReducerOutputFilename(partitionName.toString());

        reduce(partitionName, values.iterator(), context);
        uploadResults(context);
    }

    private void uploadResults(TaskTrackerContext context) {
        Hdfs hdfs = new Hdfs();
        String filename = context.getReducerOutputFilename();
        Directory directory = new Directory(context.getTaskTrackerId(),context.getJobId());
        String src = directory.getReducerOutputDirectory()+"/"+filename;
        String dest = context.getOutputPath() + "/" + filename;
        System.out.println("uploading results to S3");
        hdfs.uploadS3(src, dest);
    }

    private List<String> shuffle(List<ReducerAssignment> assignments) throws IOException {
        System.out.println(assignments);
        List<String> results = new LinkedList<String>();
        System.out.println("assignments size: " + assignments.size());
        for (int i = 0; i < assignments.size(); i++) {
            System.out.println(assignments.get(i).getPartition());
            results.addAll(readOneMapperOutputSplit(assignments.get(i)));
        }
        return results;

    }

    /**
     * read an output split of a mapper into memory
     *
     * @param reducerAssignment
     * @return
     */
    private List<String> readOneMapperOutputSplit(ReducerAssignment reducerAssignment) throws IOException {
        String assignmentIp = reducerAssignment.getServerIp();
        int assignmentPort = reducerAssignment.getServerPort();

        String srcFilePath = reducerAssignment.getFilePath();
        String filename = srcFilePath.substring(srcFilePath.lastIndexOf('/') + 1, srcFilePath.length());
        String destFilePath = this.directory.getReducerInputDirectory() +"/"+ filename;
        System.out.println("Source File Path : "+ srcFilePath);
        System.out.println("Dest File Path : " + destFilePath);

        FtpClientMR.download(assignmentIp, assignmentPort, srcFilePath, destFilePath);

        BufferedReader reader = new BufferedReader(new FileReader(destFilePath));

        List<String> result = new LinkedList<String>();
        String line = reader.readLine();
        while (line != null) {
            result.add(line);
            line = reader.readLine();
        }
        return result;

    }
}
