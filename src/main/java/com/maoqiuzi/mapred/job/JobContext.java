package com.maoqiuzi.mapred.job;

//import common.ContextInterface;

import com.maoqiuzi.mapred.common.ContextInterface;

/**
 * Job Context is the environment of Job, it is responsible for reading configuration from 
 */
public class JobContext implements ContextInterface {

	public boolean succeed;
	private Configuration config;
	private String mapper;
	private String reducer;
	private String inputPath;
	private String outputPath;
	private String jarFileName;
	private String jobId;

	public JobContext() {
		jobId = Long.valueOf(System.currentTimeMillis()).toString();
	}

	public String getJobId() {
		return jobId;
	}

	public String getMapper() {
		return mapper;
	}

	public void setMapper(String mapper) {
		this.mapper = mapper;
	}

	public String getReducer() {
		return reducer;
	}

	public void setReducer(String reducer) {
		this.reducer = reducer;
	}

	public boolean isSucceed() {
		return succeed;
	}

	public void setSucceed(boolean succeed) {
		this.succeed = succeed;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}


	
	
	/**
	 * For a job, it needs to know the master server's IP address and port, 
	 * in order to send it's mapper and reducer class to the master server.
	 * 
	 */
	public void setConf(Configuration conf) {
		this.config = conf;
	}


	public String getJarFileName() {
		return jarFileName;
	}

	public void setJarFileName(String jarFileName) {
		this.jarFileName = jarFileName;
	}
}
