package com.maoqiuzi.mapred.tasktracker;

import java.io.Serializable;

/*
 * this assignment includes all the informations 
 * needed for a reducer to retrieve a reducer input
 */
public class ReducerAssignment implements Serializable{
	private String partition;
	private String filePath;
	private String serverIp;
	private int serverPort;
	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getServerIp() {
		return serverIp;
	}

	public void setServerIp(String serverIp) {
		this.serverIp = serverIp;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public boolean equals(Object object){
		if(object instanceof ReducerAssignment){
			ReducerAssignment ra = (ReducerAssignment)object;
			return this.partition.equals(ra.getPartition())
					&&this.filePath.equals(ra.getFilePath())
					&&this.serverIp.equals(ra.getServerIp())
					&&this.serverPort == ra.getServerPort();
		} else {
			return false;
		}
	}

}
