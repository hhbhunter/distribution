package com.stp.distribution.client;
/**
 * @author hhbhunter
 */
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.stp.distribution.entity.LogEntity;
import com.stp.distribution.entity.ProcessKey;
import com.stp.distribution.entity.TaskType;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZKConfig;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkException;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.user.TaskCache;
import com.stp.distribution.util.CmdExec;
import com.stp.distribution.util.UtilTool;

public class ZkClientTask {
	public static Map<String,TaskExceute> exeMap=Maps.newConcurrentMap();
	private static final Logger clientLOG = LoggerFactory.getLogger(ZkClientTask.class);
	CuratorFramework zkInstance;
	String myClientPath;
	String myip=UtilTool.getLocalIp();
	String myLogPath;
	public ZkClientTask(CuratorFramework zkInstance,String myClientPath){
		this.zkInstance=zkInstance;
		this.myClientPath=myClientPath;
		myLogPath=ZKPaths.makePath(myClientPath, "log");
	}

	public void taskProcess(PathChildrenCacheEvent event,Map<String,ZkTask> map,String myip) throws Exception{
		ZkTask task = new ZkTask();
		String dataPath = null;
		try {
			dataPath=event.getData().getPath();
			clientLOG.info("datapath="+dataPath);
			String taskjson=new String(event.getData().getData(),ZKConfig.getZkCharset());
			clientLOG.info("taskjson==="+taskjson);
			task = TaskCache.json2zktask(taskjson);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		switch (event.getType()) {

		case CHILD_ADDED:
			if(!ZkDataUtils.isExists(dataPath)){
				throw new ZkException(dataPath+"is not exist !!!");
			}
			if(dataPath.contains("log"))break;
			clientLOG.info("client CHILD_ADDED data ==== "+event.getData().getPath());
			updateMonitorNum(1, task.getType(),myip);
			map.put(task.getTaskid(), task);
			clientZkTaskEvent(task);
			break;
		case CHILD_REMOVED:
			if(dataPath.contains("log"))break;
			updateMonitorNum(-1, task.getType(),myip);
			map.remove(task.getTaskid());
			clientLOG.info("client CHILD_REMOVED data:"+event.getData().getPath());
			break;
		case CHILD_UPDATED:
			//任务状态变更
			clientZkTaskEvent(task);
			clientLOG.info("client CHILD_UPDATED data:"+event.getData().getPath()+ " stat:"+task.getStat());
			break;
		default:

			break;
		}
	}
	/**
	 * client task process operator
	 * @param task
	 * @throws Exception
	 */
	private void clientZkTaskEvent(ZkTask task) throws Exception {

		String myProcessStatPath=ZKPaths.makePath(ZkTaskPath.getProcessPath(task.getType()), String.valueOf(task.getTaskid()));
		clientLOG.info("myProcessStatPath="+myProcessStatPath+" stat="+task.getStat());

		if(!ZkDataUtils.isExists(myProcessStatPath)){
			clientLOG.error(myProcessStatPath+" is not exists !!!");
			return;
		}	
		String myClientProcessPath=ZKPaths.makePath(myProcessStatPath, myip);


		switch (ZkTaskStatus.valueOf(task.getStat())) {
		case check:

			//			ZkDataUtils.setKVData(myClientProcessPath, ProcessKey.STAT, ZkTaskStatus.create.name());
			break;
		case success:
			//process task update
			ZkDataUtils.setKVData(myClientProcessPath, ProcessKey.STAT, ZkTaskStatus.success.name());

			break;
		case fail:
			//process rewrite stat
			ZkDataUtils.setKVData(myClientProcessPath, ProcessKey.STAT, ZkTaskStatus.fail.name());

			break;
		case start:
			//client task stat update
			//执行命令
			ZkDataUtils.setKVData(myClientProcessPath, ProcessKey.STAT, ZkTaskStatus.running.name());
			new TaskExceute(task).start();
			break;
		case stop:
			//client重启后任务存在但没有exe对象，需要自动去停
			if(exeMap.get(task.getTaskid())==null){
				System.out.println("stop命令，map不包含此任务=="+task.getTaskid());
				new TaskExceute(task).start();
				break;
			}
			System.out.println("stop命令，map包含此任务=="+task.getTaskid());
			TaskExceute stopTask=exeMap.get(task.getTaskid());
			//人工停止
			stopTask.taskFinish=true;
			stopTask.stopTask();
			clientZkTaskEvent(stopTask.getTask());
			break;
		case pause:
			//					ZkDataUtils.setKVData(myClientProcessPath, ProcessKey.STAT, ZkTaskStatus.pause.name());
			break;
		case pending:
			ZkDataUtils.setKVData(myClientProcessPath, ProcessKey.STAT, ZkTaskStatus.pending.name());
			break;
		case finish:
			//client task del

			ZkDataUtils.setKVData(myClientProcessPath, ProcessKey.STAT, ZkTaskStatus.finish.name());
			String myTaskPath=ZKPaths.makePath(ZkTaskPath.getClientTaskPath(task.getType(), myip), String.valueOf(task.getTaskid()));
			String myTaskLogPath=ZKPaths.makePath(myLogPath, String.valueOf(task.getTaskid()));
			if(ZkDataUtils.isExists(myTaskPath)){
				ZkDataUtils.removeDataPath(myTaskPath);
				clientLOG.info("remove the path="+myTaskPath);
			}
			if(ZkDataUtils.isExists(myTaskLogPath)){
				ZkDataUtils.removeDataPath(myTaskLogPath);//移除任务日志
			}
			break;
		default:
			break;
		}

	}

	public synchronized void updateMonitorNum(int num,String type,String myip) throws Exception{
		//		String clientPath=ZkTaskPath.getClientTaskPath(type, myip);
		String registPath=ZKPaths.makePath(ZkTaskPath.getMonitorPath(type), myip);
		String confPath=ZkTaskPath.getClientTaskPath(type, myip);
		String exenum=ZkDataUtils.getData(registPath);
		System.out.println("current exeNum == "+exenum);
		exenum=String.valueOf(Integer.valueOf(exenum)+num);
		System.out.println("update exeNum == "+exenum);
		ZkDataUtils.setData(registPath, exenum);
		ZkDataUtils.setData(confPath, exenum);
	}

	public void retrieveTask(TaskType type){
		String typePath=ZKPaths.makePath(myClientPath, type.name());
		try {
			List<String> tasks=ZkDataUtils.getChildren(typePath);
			for(String task:tasks){
				exeMap.put(task,null);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * process更新任务状态，通知各个client
	 * 只包含start、stop、finish
	 */

	class TaskExceute extends Thread{
		boolean taskFinish=false;
		ZkTask task;
		ZkClientLog clilog=null;
		CmdExec exe =new CmdExec();

		public ZkTask getTask() {
			return task;
		}
		public void setTask(ZkTask task) {
			this.task = task;
		}


		public TaskExceute(ZkTask task) {
			this.task=task;
		}
		@Override
		public void run() {
			switch (ZkTaskStatus.valueOf(task.getStat())) {
			case stop:
				stopTask();
				break;
			case start:
				startTask();
				break;

			default:
				break;
			}
			try {
				if(!taskFinish)
					clientZkTaskEvent(task);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void startTask(){

			clientLOG.info("Client start execute cmd="+task.getStartCmd()+" cmdPath="+task.getCmdPath());
			FutureTask<LogEntity> futureTask = null;

			try {
				exeMap.put(task.getTaskid(), this);

				switch (TaskType.valueOf(task.getType())) {
				case PERFORME:
					clilog=exeLogListen();
					futureTask = new FutureTask<LogEntity>(clilog);
					new Thread(futureTask).start();
					break;
				default:
					break;
				}
				int stat=exe.cmdExec(task.getStartCmd(),null,new File(task.getCmdPath()),true);
				//外部stop
				/*if(taskFinish){
					clientLOG.info(task.getTaskid() + " is user stop !!");
					if(!clilog.logData.isFinish()){
						clilog.logData.setFinish(true);
					}
					task.setStat(ZkTaskStatus.success.name());
					//					ZkDataUtils.setData(task.getZkpath(), task.convertJson());
					return;
				}*/
				if(stat==0){
					task.setStat(ZkTaskStatus.success.name());

				}else{
					
					if(clilog.logData.isAuto()){
						task.setStat(ZkTaskStatus.success.name());
					}else{
						task.setStat(ZkTaskStatus.fail.name());
					}
					clilog.logData.setFinish(true);
					System.out.println("set logListen true !!");
				}

				if(futureTask!=null){
					futureTask.cancel(true);//意义不大，阻塞IO时不起作用
				}


			} catch (Exception e) {
				// TODO Auto-generated catch block
				clilog.logData.setFinish(true);
				task.setStat(ZkTaskStatus.fail.name());
				task.setLog(e.getMessage());
				e.printStackTrace();
			}
		}

		public ZkClientLog exeLogListen(){
			LogEntity logData =new LogEntity();
			logData.setTaskId(task.getTaskid());
			logData.setPath(task.getCmdPath()+"/log/"+task.getTaskid()+".log");
			String myTaskLogPath=ZKPaths.makePath(myLogPath, String.valueOf(task.getTaskid()));
			ZkClientLog cliLog=new ZkClientLog(logData,exe,myTaskLogPath);

			return cliLog;
		}
		public int exeStopCmd(){
			int stat=-1;
			if(task.getStopCmd().contains("|")){
				stat=exe.cmdExec(new String[]{"sh","-c",task.getStopCmd()},null,new File(task.getCmdPath()),true);
			}else{
				stat=exe.cmdExec(task.getStopCmd(),null,new File(task.getCmdPath()),true);
			}
			return stat;
		}

		public void stopTask() {

			switch (TaskType.valueOf(task.getType())) {
			case AUTO:
				task.setStat(ZkTaskStatus.finish.name());
				break;
			case PERFORME:

				if(!clilog.logData.isFinish()){
					clilog.logData.setFinish(true);
				}
				if(!new File(task.getCmdPath()).exists()){
					task.setLog(task.getCmdPath()+" is not exist !!");
					clientLOG.info(task.getCmdPath()+" is not exist !!");
					task.setStat(ZkTaskStatus.finish.name());
					break;
				}
				if(!new File(task.getCmdPath()+File.separator+task.getTaskid()+".pid").exists()){
					task.setLog(task.getCmdPath()+File.separator+task.getTaskid()+".pid"+" is not exist !!");
					clientLOG.info(task.getCmdPath()+File.separator+task.getTaskid()+".pid"+" is not exist !!");
					task.setStat(ZkTaskStatus.finish.name());
					break;
				}
				try {
					int stat=exeStopCmd();
//					if(task.getStopCmd().contains("|")){
//						stat=exe.cmdExec(new String[]{"sh","-c",task.getStopCmd()},null,new File(task.getCmdPath()),true);
//					}else{
//						stat=exe.cmdExec(task.getStopCmd(),null,new File(task.getCmdPath()),true);
//					}
					if(stat==0){
						clientLOG.info("PERFORME task id "+task.getTaskid()+" stop ok !!");
						task.setStat(ZkTaskStatus.finish.name());

					}else{
						clientLOG.error("PERFORME task id "+task.getTaskid()+" stop failed !!");
						task.setStat(ZkTaskStatus.pending.name());
						task.setLog(exe.getErrorInf());
					}

				} catch (Exception e) {
					// TODO Auto-generated catch block
					task.setStat(ZkTaskStatus.finish.name());
					task.setLog(e.getMessage());
					e.printStackTrace();
				}
				break;

			default:
				break;
			}
			if(exeMap.get(task.getTaskid())!=null){

				CmdExec exe=exeMap.get(task.getTaskid()).exe;
				if(exe!=null)
					exe.stop();
			}
		}

	}

}
