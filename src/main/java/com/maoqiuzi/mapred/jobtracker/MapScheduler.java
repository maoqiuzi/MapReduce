package com.maoqiuzi.mapred.jobtracker;

//import job.JobContext;
//import tasktracker.ReducerAssignment;
//import tasktracker.SlaveStatus;
//import tasktracker.TaskTrackerContext;
//import tasktracker.TaskType;

import com.maoqiuzi.mapred.job.JobContext;
import com.maoqiuzi.mapred.tasktracker.SlaveStatus;
import com.maoqiuzi.mapred.tasktracker.TaskType;
import com.maoqiuzi.mapred.tasktracker.ReducerAssignment;
import com.maoqiuzi.mapred.tasktracker.TaskTrackerContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Qiudi on 2015/4/17.
 * Modified by Wang Jinpeng on 2015/4/19
 */
public class MapScheduler {
    private static long mapLimitTime = 100000;
    private JobContext jobContext = null;
    private List<InputSplit> inputSplitList;
    private int totalSucceeded = 0;
    private List<TaskTrackerAgent> taskTrackerAgentlist;
    private Map<String, List<ReducerAssignment>> mapperResults;

    public MapScheduler(JobContext jobContext, List<TaskTrackerAgent> taskTrackerAgentlist, List<InputSplit> inputSplitList) {
        this.jobContext = jobContext;
        this.taskTrackerAgentlist = taskTrackerAgentlist;
        this.inputSplitList = inputSplitList;
        this.mapperResults = new HashMap<String, List<ReducerAssignment>>();
    }

    public Map<String, List<ReducerAssignment>> getMapperResults() {
        return this.mapperResults;
    }

    public void schedule() {
        int totalSplitsNumber = inputSplitList.size();
        System.out.println("total input split:" + totalSplitsNumber);
        totalSucceeded = 0;

        while (totalSucceeded < totalSplitsNumber) {
            System.out.println("total input split:" + totalSplitsNumber);
            System.out.println("total succeeded = " + totalSucceeded);
            System.out.println("Dispatching");
            dispatch();
            System.out.println("Waiting for completion");
            waitForComplete();
        }
        System.out.println("Schedule completed");
    }

    /**
     * this function tries to dispatch as many input splits to taskTrackerAgent,
     * each agent will be assigned 1 input split. return if either list is empty.
     */
    private void dispatch() {
        while (true) {
            TaskTrackerAgent agent = getIdleAgent(taskTrackerAgentlist);
            if (null == agent) {
                System.out.println("No available agent, cannot dispatch");
                return;
            }
            InputSplit inputSplit = popInputSplit(inputSplitList);
            if (null == inputSplit) {
                System.out.println("No available Input Split, cannot dispatch");
                return;
            }
            System.out.println("Available agent and split found...");
            TaskTrackerContext context = generateTaskContext(agent.getTaskTrackerContext(), inputSplit);
            agent.assignTask(context,jobContext.getJobId());
        }
    }

    private TaskTrackerContext generateTaskContext(TaskTrackerContext context, InputSplit inputSplit) {
        context.setInputSplit(inputSplit);
        context.setStatus(SlaveStatus.MAPREADY);
        context.setTaskType(TaskType.MAPPER);
        context.setJarFileName(this.jobContext.getJarFileName());
        context.setInputPath(this.jobContext.getInputPath());
        context.setOutputPath(this.jobContext.getOutputPath());
        context.setMapperClassName(this.jobContext.getMapper());
        return context;
    }

    /**
     * a while loop checks the status of taskTrackerAgents. If any agent's
     * status is succeeded or failed, means a task is completed
     */
    private void waitForComplete() {
        while (true) {
            TaskTrackerAgent agent;
            SlaveStatus status;
            for (int i = 0; i < taskTrackerAgentlist.size(); i++) {
                agent = taskTrackerAgentlist.get(i);
                status = agent.getTaskTrackerContext().getStatus();
                if (SlaveStatus.MAPSUCCEEDED == status || SlaveStatus.MAPFAILED == status) {
                    agentTaskCompleted(agent);
                    return;
                }
            }
        }
    }


    /**
     * an agent has finished its task, either succeeded or failed.
     * If succeeded, reset the agent to IDLE, add the mapper results
     * to this.mapperResults and totalSucceed++;
     * If failed, also reset the agent to IDLE, and add the returned
     * input split back to inputSplitList
     *
     * @param agent
     */
    private void agentTaskCompleted(TaskTrackerAgent agent) {
        System.out.println("Agent Task Completed");
        TaskTrackerContext context = agent.getTaskTrackerContext();
        SlaveStatus status = context.getStatus();
        switch (status) {
            case MAPSUCCEEDED:
                totalSucceeded++;
                List<ReducerAssignment> results = context.getMapperResult();
                mergeMapperResults(results);
                context.setStatus(SlaveStatus.IDLE);
                break;
            case MAPFAILED:
                InputSplit inputSplit = context.getInputSplit();
                inputSplitList.add(inputSplit);
                context.setStatus(SlaveStatus.IDLE);
                break;
        }
    }

    /**
     * this will set the mapperResults
     * @param assignments
     */
    private void mergeMapperResults(List<ReducerAssignment> assignments) {
        System.out.println("merging mapper results size:" + assignments.size());
        for (int i = 0; i < assignments.size(); i++) {
            ReducerAssignment assignment = assignments.get(i);
            String partition = assignment.getPartition();
            List<ReducerAssignment> reducerAssignments = mapperResults.get(partition);
            if (reducerAssignments == null) {
                reducerAssignments = new ArrayList<ReducerAssignment>();
            }
            if(!reducerAssignments.contains(assignment)) {
                reducerAssignments.add(assignment);
                System.out.println("non duplicated assignment");
            }else{
                System.out.println("duplicate assignment");
            }
            mapperResults.put(partition, reducerAssignments);
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

    private InputSplit popInputSplit(List<InputSplit> inputSplitList) {
        if (inputSplitList.size() == 0) {
            return null;
        } else {
            InputSplit result = inputSplitList.get(0);
            inputSplitList.remove(0);
            return result;
        }
    }
}