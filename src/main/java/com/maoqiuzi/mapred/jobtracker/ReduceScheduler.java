package com.maoqiuzi.mapred.jobtracker;

//import common.FtpClientMR;
//import common.FtpServerMR;
//import common.Hdfs;
//import job.JobContext;
//import tasktracker.*;

import com.maoqiuzi.mapred.job.JobContext;
import com.maoqiuzi.mapred.tasktracker.SlaveStatus;
import com.maoqiuzi.mapred.tasktracker.TaskType;
import com.maoqiuzi.mapred.tasktracker.ReducerAssignment;
import com.maoqiuzi.mapred.tasktracker.TaskTrackerContext;

import java.util.*;

/**
 * Created by Administrator on 2015/4/17.
 */
public class ReduceScheduler {
    private JobContext jobContext = null;
    private List<TaskTrackerAgent> taskTrackerAgentlist;
    private Map<String,List<ReducerAssignment>> mapperResult;
    private List<String> reducerResult;
    private int totalSucceeded = 0;
    private static long reduceLimitTime = 100000;

    public ReduceScheduler(JobContext jobContext, List<TaskTrackerAgent> taskTrackerAgentlist, Map<String, List<ReducerAssignment>> mapperResult) {
        this.jobContext = jobContext;
        this.taskTrackerAgentlist = taskTrackerAgentlist;
        this.mapperResult = mapperResult;
        this.reducerResult = new ArrayList<String>();
    }

    /*
    private void mergeReducerResult() throws FileNotFoundException,IOException{
        String outputPath = jobContext.getOutputPath();
        Iterator<String> keys = mapperResult.keySet().iterator();
        PrintWriter writer = new PrintWriter(FtpServerMR.getRootDirectory()+"/result", "UTF-8");
        for(int i=0;i< reducerResult.size();i++){
            BufferedReader br = new BufferedReader(new FileReader(reducerResult.get(i)));
            String sCurrentLine="";
            while ((sCurrentLine = br.readLine()) != null) {
                writer.println(sCurrentLine);
            }
        }
        Hdfs hdfs = new Hdfs();
        writer.close();
        hdfs.uploadS3(FtpServerMR.getRootDirectory() + "/result", outputPath + "/result");
    }*/

    public void schedule() {
        int totalPartitionNumber = mapperResult.size();
        int taskAgentNumber = this.taskTrackerAgentlist.size();
        System.out.printf("total partition number: %d", totalPartitionNumber);
        int eachPartNum =0;
        if(totalPartitionNumber%taskAgentNumber==0){
            eachPartNum=totalPartitionNumber/taskAgentNumber;
        }else{
            eachPartNum=totalPartitionNumber/taskAgentNumber+1;
        }
        totalSucceeded = 0;
        while (totalSucceeded < totalPartitionNumber) {
            System.out.print("Dispatching");
            dispatch(eachPartNum);
            System.out.print("Waiting for completion");
            waitForComplete();
        }
        System.out.println("Schedule completed");
    }

    private void waitForComplete() {
        while (true) {
            TaskTrackerAgent agent;
            SlaveStatus status;
            for (int i = 0; i < taskTrackerAgentlist.size(); i++) {
                agent = taskTrackerAgentlist.get(i);
                status = agent.getTaskTrackerContext().getStatus();
                if (SlaveStatus.REDUCESUCCEEDED == status || SlaveStatus.REDUCEFAILED == status) {
                    agentTaskCompleted(agent);
                    return;
                }
            }
        }
    }

    private void agentTaskCompleted(TaskTrackerAgent agent) {
        TaskTrackerContext context = agent.getTaskTrackerContext();
        SlaveStatus status = context.getStatus();
        switch (status) {
            case REDUCESUCCEEDED:
                totalSucceeded = totalSucceeded + context.getReducerAssignments().size();
                context.setStatus(SlaveStatus.IDLE);
                break;
            case REDUCEFAILED:
                Map<String,List<ReducerAssignment>> assignments = context.getReducerAssignments();
                Iterator<String> keyIte = assignments.keySet().iterator();
                while(keyIte.hasNext()){
                    String key = keyIte.next();
                    mapperResult.put(key,assignments.get(key));
                }
                context.setStatus(SlaveStatus.IDLE);
                break;
        }
    }

    /**
     * this function tries to dispatch as many reducer assignments to taskTrackerAgent,
     * each agent will be assigned 1 assignment(1 partition). return if either list is empty.
     */
    private void dispatch(int eachPartNum) {
        while (true) {
            TaskTrackerAgent agent = getIdleAgent(taskTrackerAgentlist);
            if (null == agent) {
                System.out.print("No available agent, cannot dispatch");
                return;
            }
            Map<String,List<ReducerAssignment>> partitions = popPartition(mapperResult,eachPartNum);
            if (partitions.size()==0) {
                System.out.print("No available Input Split, cannot dispatch");
                return;
            } else {
                TaskTrackerContext context = generateContext(agent.getTaskTrackerContext(), partitions);
                agent.assignTask(context,jobContext.getJobId());
            }
        }
    }
    private TaskTrackerContext generateContext(TaskTrackerContext context, Map<String,List<ReducerAssignment>> reducerAssignments) {
        context.setStatus(SlaveStatus.REDUCEREADY);
        context.setTaskType(TaskType.REDUCER);
        context.setJarFileName(this.jobContext.getJarFileName());
        context.setInputPath(this.jobContext.getInputPath());
        context.setOutputPath(this.jobContext.getOutputPath());
        context.setReducerClassName(this.jobContext.getReducer());
        context.setReducerAssignments(reducerAssignments);
        return context;
    }

    /**
     * @param mapperResult partitions haven't been assign to reducer
     * @param eachPartNum maximum partitions number to return
     * @return up to eachPartNum partitions as a Map<String,List<ReducerAssignment>>
     */
    private Map<String,List<ReducerAssignment>> popPartition(Map<String, List<ReducerAssignment>> mapperResult,int eachPartNum) {
        Map<String,List<ReducerAssignment>> partitionsSplit = new HashMap<String, List<ReducerAssignment>>();
        List<String> partitionsToRemove = new LinkedList<String>();
        Set<String> remainingPartitions = mapperResult.keySet();
        if (0 != remainingPartitions.size()) {
            Iterator<String> partitionNames = remainingPartitions.iterator();
            int partitionNum = 0;
            while(partitionNames.hasNext()&&partitionNum<eachPartNum){
                String partition = partitionNames.next();
                List<ReducerAssignment> result = mapperResult.get(partition);
                partitionsSplit.put(partition, result);
                System.out.println("Size of partition of mapperResults.get(partitionNumber): " + result.size());
                partitionsToRemove.add(partition);
                partitionNum++;
            }
            removePartitions(mapperResult, partitionsToRemove);
        }
        return partitionsSplit;
    }

    private void removePartitions(Map<String, List<ReducerAssignment>> map, List<String> partitionsToRemove) {
        for (int i = 0; i < partitionsToRemove.size(); i++) {
            map.remove(partitionsToRemove.get(i));
        }
    }

    private TaskTrackerAgent getIdleAgent(List<TaskTrackerAgent> taskTrackerAgentlist) {
        for (int i = 0; i < taskTrackerAgentlist.size(); i++) {
            if (SlaveStatus.IDLE == taskTrackerAgentlist.get(i).getTaskTrackerContext().getStatus()) {
                return taskTrackerAgentlist.get(i);
            }
        }
        return null;
    }


}
