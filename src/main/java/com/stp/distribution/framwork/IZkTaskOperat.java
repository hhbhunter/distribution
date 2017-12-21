package com.stp.distribution.framwork;
/**
 * @author hhbhunter
 */
import java.util.concurrent.Executor;

import com.stp.distribution.entity.ZkTask;


public interface IZkTaskOperat {
	
	/**
	 * 添加任务进队列,默认执行器
	 * @param task
	 */
	void addTask(ZkTask task);
	/**
	 * 添加任务进队列,自己执行器
	 * @param task
	 * @param executor
	 */
	void addTask(ZkTask task, Executor executor);
	
	/**
	 * 移除队列任务，包括自己执行器
	 * @param task
	 */
	void removeTask(ZkTask task);
	
	boolean stopTask(ZkTask task);
	
	void startTask(ZkTask task);

	void createTask(ZkTask task);

}
