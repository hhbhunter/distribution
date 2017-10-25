package com.stp.distribution.entity;

public enum ZkTaskStatus  {
	check(-1),
	//任务状态最终完成
	finish(0),
	//任务执行成功，未必完成
	success(1),
	create(2),
	start(3),
	running(4),
	stop(5),
	pause(6),
	pending(7),
	fail(8);
	private int index;
	private ZkTaskStatus(int priority){
		this.index=priority;
	}

	public int getPriority(){
		return this.index;
	}

}
