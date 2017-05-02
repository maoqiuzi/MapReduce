package com.maoqiuzi.mapred.jobtracker;

//import common.NetworkUtils;
//import job.JobContext;

import com.maoqiuzi.mapred.common.NetworkUtils;
import com.maoqiuzi.mapred.job.JobContext;

import java.net.Socket;

/*
 * responsible for communication with Job. Once a Job connection accepted, 
 * this job tracker is responsible for receive job context and send back 
 * final status
 */
public class JobAgent {
	private JobTracker jobTracker;
	private JobContext jobContext;
	private Socket clientSocket;

	public JobAgent(JobTracker jt,Socket clientSocket, JobContext jc){
		this.jobTracker = jt;
		this.clientSocket = clientSocket;
		this.jobContext = jc;
	}

	public JobContext getJobContext() {
		return jobContext;
	}

	public void sendJobComplete() {
		JobContext context = new JobContext();
		context.setSucceed(true);
		NetworkUtils.sendContext(clientSocket, context);
	}
}
