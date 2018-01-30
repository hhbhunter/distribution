package com.stp.distribution.entity;

import java.util.concurrent.atomic.AtomicBoolean;
/**
 * @author hhbhunter
 */
public class LogEntity {
	String path;
	String content;
	int lineNum;
	String taskId;
	// 确保线程安全
	AtomicBoolean finish=new AtomicBoolean(false);
	boolean auto=false;
	
	public boolean isAuto() {
		return auto;
	}
	public void setAuto(boolean auto) {
		this.auto = auto;
	}
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public boolean isFinish() {
		return finish.get();
	}
	public void setFinish(boolean finish) {
		this.finish.set(finish);
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public int getLineNum() {
		return lineNum;
	}
	public void setLineNum(int lineNum) {
		this.lineNum = lineNum;
	}
	public String toString(){
		return finish+" is finsh "+content;
		
	}
}
