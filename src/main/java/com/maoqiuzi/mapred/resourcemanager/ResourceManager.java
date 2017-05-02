package com.maoqiuzi.mapred.resourcemanager;


//import tasktracker.TaskTracker;

import com.maoqiuzi.mapred.tasktracker.TaskTracker;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by wangjinpeng on 4/21/15.
 */
public class ResourceManager {

    private List<TaskTracker> taskTrackers;
    private int memoryOfOS;

    public ResourceManager() {
        this.memoryOfOS = getOSMemory();
        this.taskTrackers = new LinkedList<TaskTracker>();
        launchTaskTrackers(memoryOfOS);
    }

    private void launchTaskTrackers(int num) {
        for (int i = 0; i < num; i++) {
            launchTaskTracker();
        }
    }

    /**
     * get the whole memory of the OS
     * @return the overall memory of the OS, in Gigabytes
     */
    public int getOSMemory(){
        com.sun.management.OperatingSystemMXBean bean =
                (com.sun.management.OperatingSystemMXBean)
                        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        long max = bean.getTotalPhysicalMemorySize();
        int result = (int)(max/1000/1000/1000);
        return result;
    }
    
    private void launchTaskTracker(){
        TaskTracker slave = new TaskTracker();
        Thread slaveThread = new Thread(slave);
        this.taskTrackers.add(slave);
        slaveThread.start();
    }

    public static void main(String[] args){
        ResourceManager rm = new ResourceManager();
    }

}
