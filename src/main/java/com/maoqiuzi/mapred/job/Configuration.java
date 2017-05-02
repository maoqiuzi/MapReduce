package com.maoqiuzi.mapred.job;

import java.io.Serializable;

/*
 * Configuration is responsible to get Master server's IP and port
 */
public class Configuration implements Serializable {
	private String masterIp;
	private int masterPort;
	private long jvmMemory;
	private long inputSplitSize;

	public Configuration(){
	}

	public String getMasterIp() {
		return masterIp;
	}

	public void setMasterIp(String masterIp) {
		this.masterIp = masterIp;
	}

	public int getMasterPort() {
		return masterPort;
	}

	public void setMasterPort(int masterPort) {
		this.masterPort = masterPort;
	}

	public long getJvmMemory() {
		return jvmMemory;
	}

	public void setJvmMemory(long jvmMemory) {
		this.jvmMemory = jvmMemory;
	}

	public long getInputSplitSize() {
		return inputSplitSize;
	}

	public void setInputSplitSize(long inputSplitSize) {
		this.inputSplitSize = inputSplitSize;
	}

}
