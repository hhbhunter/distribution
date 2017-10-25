package com.stp.distribution.process;

import java.util.Map;

import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;

public interface IProcess {
	
	void createNodeData(Map<String,String> data,ZkTask task);
	/**
	 * 更新任务所有相关的节点状态，包括删除操作
	 * @param data
	 * @param task
	 * @param stat
	 */
	void updateNodeData(Map<String,String> data,ZkTask task,ZkTaskStatus stat);
	
	/**
	 * process taskstat collect
	 * @param data
	 * @param task
	 */
	boolean collectStat(Map<String,String> data,ZkTask task);
	
}
