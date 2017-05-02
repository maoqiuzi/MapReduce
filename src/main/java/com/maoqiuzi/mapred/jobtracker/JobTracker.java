package com.maoqiuzi.mapred.jobtracker;

import com.maoqiuzi.mapred.common.NetworkUtils;
import com.maoqiuzi.mapred.job.JobContext;
import com.maoqiuzi.mapred.common.ContextInterface;
import com.maoqiuzi.mapred.common.Hdfs;
import com.maoqiuzi.mapred.tasktracker.ReducerAssignment;
import com.maoqiuzi.mapred.tasktracker.TaskTrackerContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

//import common.ContextInterface;
//import common.NetworkUtils;
//import job.JobContext;
//import tasktracker.*;
//import common.Hdfs;

/*
 * jobtracker 
 */
public class JobTracker {
    private Hdfs hdfs;
    private JobAgent jobAgent;
    private String masterIp;
    private int masterPort;
    private List<InputSplit> inputSplits;
    private List<TaskTrackerAgent> taskTrackerAgents;
    private Map<String, List<ReducerAssignment>> mapperResult;
    private ServerSocket serverSocket;

    public JobTracker() throws IOException {
        this.hdfs = new Hdfs();
        this.jobAgent = null;
        this.inputSplits = new ArrayList<InputSplit>();
        this.taskTrackerAgents = new ArrayList<TaskTrackerAgent>();

        this.masterPort = this.getSparePort();
        this.masterIp = this.getLocalPublicIp();
        this.setCloudIpPort(masterIp, masterPort);
        System.out.println("This ip:" + masterIp + "  This port:" + masterPort);
        System.out.println("Waiting for connection");
        serverSocket = new ServerSocket(this.masterPort);
        waitForConnection();
    }

    private JobContext getJobContext() {
        return jobAgent.getJobContext();
    }

    /**
     * use Hdfs to store the ip and port of this jobtracker, for slaves to read
     */
    private void setCloudIpPort(String masterIp, int masterPort) {
        this.hdfs.setMasterIpPort(masterIp, masterPort);
    }

    /**
     * a while loop listen to local port, wait for a connection. Once a
     * connection accepted, the counter part will send a ContextInterface. Use
     * the getContextType() to determine the type of the Context, whether it is
     * a JobContext or a TaskTracker(Slave) Context. If it is JobContext, run a
     * thread JobAgent, then break to schedule phase; else, run a thread
     * TaskTrackerAgent, and add the TaskTrackAgent into taskTrackAgents list
     */
    public void waitForConnection() {
        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                ContextInterface context = receiveContext(clientSocket);
                 if (context instanceof TaskTrackerContext) {
                    this.launchTaskTrackerAgent((TaskTrackerContext) context, clientSocket);
                } else if (context instanceof JobContext) {
                    this.launchJobAgent((JobContext) context, clientSocket);
                     System.out.println(context);
                    break;
                }
            }
            this.runJob();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * schedule mapper and wait until it's done or
     * schedule reducer and wait until it's done
     * then send Job complete.
     * Then keep on wait for connection, i.e. net round.
     */
    private void runJob() throws Exception {
        setJarFilePath();
        System.out.println("Starting to split input file...");
        generateFileSplit();
        System.out.println("Split input file finished.");
        System.out.println("Starting Mapper schedule");
        scheduleMapper();
        System.out.println("Finishing Mapper schedule");
        System.out.println("Starting Reducer schedule");
        scheduleReducer();
        System.out.println("Finishing Reducer schedule");
        sendJobComplete();
        System.out.println("Job Finished");
        waitForConnection();
    }

    private void setJarFilePath() {
        JobContext jobContext = jobAgent.getJobContext();
        System.out.println("Context: "+jobContext);
        String jarFileName = jobContext.getJarFileName();
        System.out.println("jarfile name: " + jarFileName);
        for(int i=0;i<this.taskTrackerAgents.size();i++){
            TaskTrackerContext context = taskTrackerAgents.get(i).getTaskTrackerContext();
            context.setJarFileName(jarFileName);
            taskTrackerAgents.get(i).setTaskTrackerContext(context);
        }
    }

    private void sendJobComplete() {
        jobAgent.sendJobComplete();
    }

    private ContextInterface receiveContext(Socket clientSocket) throws IOException, ClassNotFoundException {
        ContextInterface context = NetworkUtils.recvContext(clientSocket);
        return context;
    }

    private void launchJobAgent(JobContext context, Socket clientSocket) {
        jobAgent = new JobAgent(this, clientSocket, context);

    }

    private void launchTaskTrackerAgent(TaskTrackerContext context, Socket clientSocket) {
        context.print();
        TaskTrackerAgent agent = new TaskTrackerAgent(this, clientSocket, context);
        Thread slaveThread = new Thread(agent);
        this.taskTrackerAgents.add(agent);
        slaveThread.start();
    }

    /**
     * this function will read all input file in S3 and convert them into inputSplits then
     * set this.inputSplits
     */
    private void generateFileSplit() {
        JobContext context = this.getJobContext();
        String path = context.getInputPath();
        List<String> filenames = this.hdfs.getFilesInDirectory(path);
        System.out.println("all filename size: " + filenames.size());

        try {
            String lastFilename = "filename";
            String tmp = "tmp";
            long lastOffset = 0;
            long currentOffset = -1;
            int tmpOffset = 0;
            int interval = 100;
            boolean end = false;

            for (int i = 0; i < filenames.size(); ) {
                lastFilename = filenames.get(i);
                lastOffset = currentOffset + 1;
                currentOffset = lastOffset + 64000000;
                end = false;

                if(hdfs.getFileSizeS3(path + "/" + lastFilename) > 64000000) {
                    while (!end) {
                        tmp = hdfs.readS3(path + "/" + lastFilename, currentOffset, currentOffset + interval).toString();
                        if (tmp.length() <= currentOffset + interval - lastOffset) {
                            i += 1;

                            end = true;
                        } else if (tmp.contains("\n")) {
                            tmpOffset = tmp.indexOf("\n");
                            currentOffset += interval + tmpOffset;

                            end = true;
                        } else {
                            currentOffset += interval;
                        }
                    }
                }
                else {
                    i += 1;
                }

                InputSplit split = new InputSplit(this.inputSplits.size(), path, lastFilename, lastOffset, currentOffset);
                this.inputSplits.add(split);

                currentOffset = -1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * get the JobContext, split Input into InputSplits; get all active
     * TaskTrackers, schedule which is mapper schedule which InputSplits to
     * which mapper. call assignTask to change the status of the
     * TaskTrackerAgent(in order to trigger the mapper phase)
     */
    public void scheduleMapper() throws Exception{
        MapScheduler scheduler = new MapScheduler(this.getJobContext(), this.taskTrackerAgents, this.inputSplits);
        scheduler.schedule();
        this.mapperResult = scheduler.getMapperResults();
    }


    /**
     * get all mapper output file path(location), and all all TaskTrackerAgent,
     * schedule which is reducer call the assignTask to change the status of the
     * TaskTrackerAgent(in order to trigger the reducer phase)
     */
    public void scheduleReducer() throws Exception{
        ReduceScheduler scheduler = new ReduceScheduler(this.getJobContext(), this.taskTrackerAgents, this.mapperResult);
        scheduler.schedule();
    }

    /**
     * a while loop to check whether all TaskTrackerAgent's status is Reducer
     * finished, if so, call JobAgent.
     * public void waitForAllReducerComplete() {
     * <p/>
     * }
     */

    public static void main(String[] args) throws IOException {
        JobTracker jobTracket = new JobTracker();
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

    /**
     * get Port Num using random number in (1024, 65535)
     *
     * @return Port Num
     */
    private int getSparePort() {
        Random rand = new Random();
        return rand.nextInt((65535 - 1024) + 1) + 1024;
    }
}
