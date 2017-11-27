package com.stp.distribution.user;

import java.util.concurrent.Executor;

import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.IZkTaskOperat;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkException;
import com.stp.distribution.framwork.ZkTaskPath;
/**
 * 
 * @author hhbhunter
 *
 */
public class ZkTaskOperatImp implements IZkTaskOperat,Runnable {

	private static final Logger taskOpLOG = LoggerFactory.getLogger(ZkTaskOperatImp.class);

	@Override
	public void addTask(ZkTask task) {
		// TODO Auto-generated method stub
		TaskCache.addTask(task);
	}

	@Override
	public void addTask(ZkTask task, Executor executor) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeTask(ZkTask task) {
		try {
			ZkDataUtils.removeDataPath(task.getZkpath());
			// ZK执行完成任务，删除节点，删除缓存信息
			TaskCache.delTask(task.getTaskid());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			taskOpLOG.error(e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void stopTask(ZkTask task) {
		//del all taskid
		//停止任务，删除
		String taskPath=task.getZkpath();
		if(!taskPath.equalsIgnoreCase("")){
			task.setStat(ZkTaskStatus.stop.name());
			try {
				ZkDataUtils.setData(taskPath, task.convertJson());
				//				ZkDataUtils.setData(taskPath, "");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else{
			TaskCache.removeTask(task);
		}

	}

	@Override
	public void startTask(ZkTask task) {
		// TODO Auto-generated method stub

	}
	@Override
	public void createTask(ZkTask task) {
		try {
			String path=ZkTaskPath.getControllPath(task.getType());
			path=ZkDataUtils.creatPersSequePath(ZKPaths.makePath(path,task.getType()));
			if(ZkDataUtils.isExists(path)){
				task.setZkpath(path);
				ZkDataUtils.setData(path, task.convertJson());
				taskOpLOG.info(task.getTaskid()+" controll path create sucess !!!");
				TaskCache.updateTask(task);
			}else{

				throw new ZkException(task.getTaskid()+" path create failed !!! ");
			}
		} catch (Exception e) {
			TaskCache.updateTaskQueue(task);
			taskOpLOG.error(e.getMessage());
		}

	}

	@Override
	public void run() {
		taskOpLOG.info("taskExcute start.....");
		ZkTask task = null;
		while(true){
			try {
				task = TaskCache.TASKQUEUE.take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(task!=null){
				createTask(task);
			}
		}
	}
}
