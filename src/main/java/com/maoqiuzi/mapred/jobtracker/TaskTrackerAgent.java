package com.maoqiuzi.mapred.jobtracker;

import com.maoqiuzi.mapred.tasktracker.SlaveStatus;
import com.maoqiuzi.mapred.tasktracker.TaskTrackerContext;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class TaskTrackerAgent implements Runnable {
    private JobTracker jobTracker;
    private TaskTrackerContext taskTrackerContext;
    private Socket clientSocket;
    //private final Lock statusLock = new statusLock

    public TaskTrackerAgent(JobTracker jt, Socket clientSocket, TaskTrackerContext ttc) {
        this.jobTracker = jt;
        this.clientSocket = clientSocket;
        this.taskTrackerContext = ttc;
    }

    public TaskTrackerContext getTaskTrackerContext() {
        return taskTrackerContext;
    }

    public void setTaskTrackerContext(TaskTrackerContext taskTrackerContext) {
        this.taskTrackerContext = taskTrackerContext;
    }

    /**
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     * a while loop to check the status of self, if it is idle, loop continue;
     * if it is ready to start mapper, it will call sendTaskContext to actually
     * send the input split(path) in TaskTrackerContext to TaskTracker via clientSocket
     * then continue loop.
     */
    public void run() {
        int count = 0;
        // TODO Auto-generated method stub
        while (true) {
            SlaveStatus status = this.getTaskTrackerContext().getStatus();
            if (status == SlaveStatus.REDUCEREADY) {
                System.out.println("Reducer ready. Sending context");
                this.sendTaskContext();
                System.out.println("Reducer context sent");
                this.waitForReducerComplete();
            } else if (status == SlaveStatus.MAPREADY) {
                System.out.println("Mapper ready. Sending context");
                this.sendTaskContext();
                System.out.println("Mapper context sent");
                this.waitForMapperComplete();
            } else {
                synchronized (this) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * set the input splits and task type(map or reduce),
     * and set the status of this TaskTrackAgent to ready to dispatch tasks
     * the while loop will inspect this status, and find it is ready to dispatch
     * tasks to TaskTrackers, thus will send TaskContext to TaskTrackers
     * generate TaskTrackerContext in JobTracker scheduleMapper()
     *
     * @param context(informatin we need: input splits,tasktype,slavestatus,jarFileName,inputPath,outputPath,MapperClassName)
     */
    public void assignTask(TaskTrackerContext context,String jobId) {
        System.out.println("assigning: " + context.getStatus().toString());
        this.taskTrackerContext = context;
        this.taskTrackerContext.setJobId(jobId);
        System.out.println("assigned: " + this.getTaskTrackerContext().getStatus().toString());
        System.out.println(SlaveStatus.MAPREADY == this.getTaskTrackerContext().getStatus());
        synchronized (this) {
            notify();
        }
    }

    /**
     * send the context to TaskerTracker, and set the status of self to be waiting for
     * mapper finish. call wait for mapper complete
     */
    private void sendTaskContext() {
        try {
            ObjectOutputStream stream = new ObjectOutputStream(clientSocket.getOutputStream());
            stream.writeObject(this.taskTrackerContext);
        } catch (IOException ie) {
            System.err.println("Exception occur when send TaskTrackerContext.");
            ie.printStackTrace();
        }
    }

    private TaskTrackerContext receiveTaskContext(Socket clientSocket) {
        try {
            ObjectInputStream stream = new ObjectInputStream(clientSocket.getInputStream());
            TaskTrackerContext context = (TaskTrackerContext) stream.readObject();
            return context;
        } catch (ClassNotFoundException cnfe) {
            System.err.println("Exception ClassNotFound occur when receive TaskTrackerContext.");
            cnfe.printStackTrace();
            return null;
        } catch (IOException ie) {
            System.err.println("Exception IOException occur when receive TaskTrackerContext.");
            ie.printStackTrace();
            return null;
        }
    }

    /**
     * a while loop to receive TaskTrackerContext, check whether it is ongoing,
     * failed or succeeded. if succeeded, set self status to mapper succeeded.
     * send the TaskTrackerContext back to JobTracker, JobTracker will assign
     * new task to this agent. thus call run to wait for that assignment.
     */
    private void waitForMapperComplete() {
        while (true) {
            this.taskTrackerContext = this.receiveTaskContext(clientSocket);
            if (this.taskTrackerContext.getStatus() == SlaveStatus.MAPFAILED ||
                    this.taskTrackerContext.getStatus() == SlaveStatus.MAPSUCCEEDED) {
                System.out.println("Mapper completed. It is either failed or succeeded.");
                break;
            } else {
                System.out.println(taskTrackerContext.getStatus());
            }
        }
    }

    /**
     * a while loop to receive TaskTrackerContext, check whether it is ongoing,
     * failed or succeeded. if succeeded, set self status to reducer succeeded.
     * send the TaskTrackerContext back to JobTracker, JobTracker will assign
     * new task to this agent. thus call run to wait for that assignment.
     */
    private void waitForReducerComplete() {
        while (true) {
            this.taskTrackerContext = this.receiveTaskContext(clientSocket);
            if (this.taskTrackerContext.getStatus() == SlaveStatus.REDUCEFAILED ||
                    this.taskTrackerContext.getStatus() == SlaveStatus.REDUCESUCCEEDED) {
                System.out.print("Reducer completed. It is either failed or succeeded.");
                break;
            }
        }
    }

}
