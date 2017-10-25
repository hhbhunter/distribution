package com.stp.distribution.user;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.stp.distribution.entity.ZkTask;

public class TaskCache {
	public static Map<Integer,ZkTask> taskCache=Maps.newConcurrentMap();
	public static LinkedBlockingQueue<ZkTask> TASKQUEUE=new LinkedBlockingQueue<ZkTask>();
	public static Map<Integer,ZkTask> stopCache=Maps.newConcurrentMap();
	
	public static ZkTask getTask(int taskid){
		return taskCache.get(taskid);
	}

	public static void addTask(ZkTask task){
		if(!taskCache.containsKey(task.getTaskid())){
			taskCache.put(task.getTaskid(), task);
			TASKQUEUE.add(task);
		}
		
	}
	
	public static void updateTaskQueue(ZkTask task){
		if(!TASKQUEUE.contains(task)){
			TASKQUEUE.add(task);
		}
		
	}

	public static void delTask(int taskid){
		taskCache.remove(taskid);
	}

	public static boolean  updateTask(ZkTask task){
		if(taskCache.containsKey(task.getTaskid())){
			taskCache.put(task.getTaskid(), task);
			return true;
		}
		return false;
	}
	public static ZkTask pollTask(){
		return   TASKQUEUE.poll();
	}
	
	public static void removeTask(ZkTask task) {
		if(taskCache.containsKey(task.getTaskid()))
		TASKQUEUE.remove(taskCache.get(task.getTaskid()));
	}

	
	public static LinkedBlockingQueue<ZkTask> getQueue(){
		return TASKQUEUE;
	}

	
	//预留执行器
	public void addTask(ZkTask task, Executor executor) {
		
	}
	
	public static ZkTask json2zktask(String jsondata){
		if(jsondata==null || jsondata.equals("")){
			return null;
		}
		
		return JSON.parseObject(jsondata, ZkTask.class);
	}
}
