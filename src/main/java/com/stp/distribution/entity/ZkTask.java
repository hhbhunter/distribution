package com.stp.distribution.entity;
/**
 * @author hhbhunter
 */
import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.JSON;

public class ZkTask{
	
	
	public String type=""; 
	public String taskid="-1";
	public String startCmd="";
	public String stopCmd="";
	public String cmdPath="";
	public List<String> client=new ArrayList<String>();
	public String stat="";
	public String zkpath="";
	public int exeNum=1;
	public String log="";

	public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
	}

	public String getStartCmd() {
		return startCmd;
	}

	public void setStartCmd(String startCmd) {
		this.startCmd = startCmd;
	}

	public String getStopCmd() {
		return stopCmd;
	}

	public void setStopCmd(String stopCmd) {
		this.stopCmd = stopCmd;
	}

	public int getExeNum() {
		return exeNum;
	}

	public void setExeNum(int exeNum) {
		this.exeNum = exeNum;
	}

	public List<String> getClient() {
		return client;
	}

	public void setClient(List<String> client) {
		this.client = client;
	}

	public String getCmdPath() {
		return cmdPath;
	}

	public void setCmdPath(String cmdPath) {
		this.cmdPath = cmdPath;
	}

	public String getType() {
		return type;
	}

	public String getTaskid() {
		return taskid;
	}

	public String getStat() {
		return stat;
	}

	public String getZkpath() {
		return zkpath;
	}

	public void setZkpath(String zkpath) {
		this.zkpath = zkpath;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}

	public void setStat(String stat) {
		this.stat = stat;
	}
	
	public String convertJson(){
		return JSON.toJSONString(this);
	}
	
	
}
