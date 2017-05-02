package com.maoqiuzi.mapred.common;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by wangjinpeng on 4/22/15.
 */
public class Directory implements Serializable{
    private String taskTrackerRootDirectory;
    private String jobRootDirectory;
    private String mapperOutputDirectory;
    private String reducerInputDirectory;
    private String reducerOutputDirectory;
    private String jobJarFileDirectory;

    public String getUserDirectory() {
        return userDirectory;
    }

    private String userDirectory;

    public Directory(String taskTrackerId) {
        initTaskTrackerDirectory(taskTrackerId);
        initUserDirectory();
    }

    public Directory(String taskTrackerId, String jobId) {
        initTaskTrackerDirectory(taskTrackerId);
        initUserDirectory();
        initJobRootDirectory(jobId);
    }

    private void initUserDirectory() {
        this.userDirectory = taskTrackerRootDirectory + "/users";
        File rootDir = new File(userDirectory);
        if (!rootDir.exists()) {
            cleanAndMakeDirecoty(userDirectory);
        }
    }

    /**
     * on call, this will check whether the tasktracker dir is already exist,
     * if so, do nothing. else, init task tracker directory
     * @param taskTrackerId
     */
    private void initTaskTrackerDirectory(String taskTrackerId) {
        String taskTrackerDirName = "taskRoot-" + taskTrackerId;
        this.taskTrackerRootDirectory = System.getProperty("user.home") + "/" + taskTrackerDirName;
        File rootDir = new File(taskTrackerRootDirectory);
        if (!rootDir.exists()) {
            cleanAndMakeDirecoty(taskTrackerRootDirectory);
        }
    }

    /**
     * on call, this will check whether the job dir is already exist,
     * if so, do nothing. else, init the Job directory.

     * @param jobId
     */
    public void initJobRootDirectory(String jobId) {
        this.jobRootDirectory = this.taskTrackerRootDirectory + "/Job-" + jobId;
        this.mapperOutputDirectory = this.jobRootDirectory + "/mapOutput";
        this.reducerInputDirectory = this.jobRootDirectory + "/reduceInput";
        this.reducerOutputDirectory = this.jobRootDirectory + "/reduceOutput";
        this.jobJarFileDirectory = this.jobRootDirectory + "/jobJarFile";
        File rootDir = new File(jobRootDirectory);
        if (!rootDir.exists()) {
            cleanAndMakeDirecoty(jobRootDirectory);
            cleanAndMakeDirecoty(mapperOutputDirectory);
            cleanAndMakeDirecoty(reducerInputDirectory);
            cleanAndMakeDirecoty(reducerOutputDirectory);
            cleanAndMakeDirecoty(jobJarFileDirectory);
        }
    }

    private void cleanAndMakeDirecoty(String directory) {
        File dir = new File(directory);
        if (dir.exists()) {
            try {
                FileUtils.cleanDirectory(dir);
            } catch (IOException e) {
                System.err.println("Exception occur when cleaning " + directory + " directory");
                e.printStackTrace();
            }
        } else {
            dir.mkdir();
        }
    }

    public String getTaskTrackerRootDirectory() {
        return taskTrackerRootDirectory;
    }

    public String getJobRootDirectory() {
        return jobRootDirectory;
    }

    public String getMapperOutputDirectory() {
        return mapperOutputDirectory;
    }

    public String getReducerInputDirectory() {
        return reducerInputDirectory;
    }

    public String getReducerOutputDirectory() {
        return reducerOutputDirectory;
    }

    public String getJobJarFileDirectory() {
        return jobJarFileDirectory;
    }

}
