package com.stp.distribution.process;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.stp.distribution.entity.ProcessKey;
import com.stp.distribution.entity.TaskType;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZKConfig;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.manager.ZkRegistMonitor;
import com.stp.distribution.manager.ZkTaskDistribution;
import com.stp.distribution.manager.ZkTaskManager;
import com.stp.distribution.util.StringUtils;
/**
 * 
 * @author hhbhunter
 *
 */
public class ProcessTaskOperate {
	private static final Logger processLOG = LoggerFactory.getLogger(ProcessTaskOperate.class);
	
	public static ZkTask getTaskByPath(String controllTaskPath){
		String task = null;
		try {
			task = ZkDataUtils.getData(controllTaskPath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			processLOG.error("controllTaskPath is error !!" +controllTaskPath);
			e.printStackTrace();
		}
		if(StringUtils.isEmpty(task)){
			return null;
		}
		processLOG.debug("process controllTaskPath == "+task);
		return JSON.parseObject(task,ZkTask.class);
	}

	public static String getCurrentTaskIndex(String type) throws Exception{
		String indexPath=ZKPaths.makePath(ZkTaskPath.getProcessPath(type), ZkTaskPath.INDEX_PATH);
		return ZkDataUtils.getKVData(indexPath,ZkTaskPath.INDEX_PATH);
	}

	public static String genrateNewIndex(String type,String newIndex){
		String taskPath="";
		for(int i=0;i<10-newIndex.length();i++){
			taskPath=taskPath+"0";
		}
		taskPath=type+taskPath+newIndex;
		return taskPath;
	}
	public static boolean currVsNewIndex(String currIndex,String newindex,String type){
		return Long.valueOf(currIndex.substring(type.length()))<Long.valueOf(newindex.substring(type.length()));
	}

	public static void updateNodeData(Map<String, String> data,ZkTask task,ZkTaskStatus stat) {
		switch (stat) {
			
		case success:
			//update db
			//del client task
			//del process task
			try {
				opsucessTask(data,task);
			} catch (Exception e1) {
				e1.printStackTrace();
			}

			break;
		case pending:
		case fail:

			opfailTask(data, task);
			break;
		case stop:

			break;
		case create:
		case running:
			//do nothing
			processLOG.info("++++++++++"+task.getTaskid()+"+++++"+stat.name());
			break;
		case finish:
			String procTaskPath=ZKPaths.makePath(ZkTaskPath.getProcessPath(task.getType()), String.valueOf(task.getTaskid()));
			try {
				processLOG.info("process remove  finish stat ==== "+procTaskPath );
				ZkDataUtils.removeDataPathAndChildren(procTaskPath);
				//				ZkDataUtils.removeDataPath(procTaskPath);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		default:
			break;
		}

	}


	public static void zombieTask(ZkTask task,String processPath){
		try {
			Map<String,String> zombieProcess=ZkDataUtils.getMapData(processPath);
			String currIndex=ZkDataUtils.getKVData(ZKPaths.makePath(ZkTaskPath.getProcessPath(task.getType()), ZkTaskPath.INDEX_PATH), ZkTaskPath.INDEX_PATH);
			String zombiePath=zombieProcess.get(ProcessKey.SRC);
			Set<String> keys=zombieProcess.keySet();
			keys.remove(ProcessKey.SRC);
			ZkTask zombieTask=getTaskByPath(zombiePath);
			String newPath=task.getZkpath();
			if(!newPath.equals(zombiePath)||currVsNewIndex(currIndex, ZKPaths.getNodeFromPath(zombiePath), task.getType())){
				System.out.println("======================");
				for(String client:keys){
					System.out.println("client==="+client);
					String clientTaskPath=ZKPaths.makePath(ZkTaskPath.getClientTaskPath(task.getType(), client),String.valueOf(task.getTaskid()));
					if(ZkDataUtils.isExists(clientTaskPath)){
						operatCliTask(zombieProcess, zombieTask, client);
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public static boolean createProcessTask(ZkTask task) throws Exception{
		String processPath=ZKPaths.makePath(ZkTaskPath.getProcessPath(task.getType()), String.valueOf(task.getTaskid()));
		if(ZkDataUtils.isExists(processPath)){
			processLOG.error("process "+task.getType()+" taskid = "+task.getTaskid()+" is exists !!");
			// 当前会跳过已存在任务，往下执行，有可能产生僵尸任务
						zombieTask(task,processPath);
			return false;
		}
		if(!ZkTaskManager.choiceClient2task(task)){
			processLOG.error("process "+task.getType()+" taskid = "+task.getTaskid()+" choice client failed !!");
			return false;
		}

		//重复检查
		if(!processTaskCheck(task)){
			processLOG.error("process "+task.getType()+" taskid = "+task.getTaskid()+" check client failed !!");
			return false;
		}
		Map<String,String> myprocess=new HashMap<>();
		myprocess.put(ProcessKey.SRC, task.getZkpath());
		for(String client:task.getClient()){
			myprocess.put(client,task.getStat());
		}
		ZkDataUtils.createDataPath(processPath, JSON.toJSONString(myprocess));
		for(String client:task.getClient()){
			ZkDataUtils.createDataPath(ZKPaths.makePath(processPath, client), "{\"stat\":\"create\"}");
		}
		return true;
	}

	public static boolean processTaskCheck(ZkTask zktask){
		for(String client:zktask.getClient()){
			if(!checkClientConf(client,zktask.getType())){
				return false;
			}
		}
		return true;
	}
	/**
	 * client 并发数是否满足
	 * @param client
	 * @param type
	 * @return
	 */
	public static boolean checkClientConf(String client,String type){
		int conf=9999;//不存在client情况
		processLOG.info("process get client path = "+ZkTaskPath.getClientTaskPath(type, client));
		try {
			conf=Integer.valueOf(new String(ZkDataUtils.getData(ZkTaskPath.getClientTaskPath(type, client))));
			processLOG.info("client conf num = "+conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			processLOG.error(e.getMessage());
			e.printStackTrace();
		}
		if(conf<Integer.valueOf(ZKConfig.getClientExeNum())){
			return true;
		}
		return false;
	}
	private static void updateDB(ZkTask task) {
		try {
			ZkDataUtils.setData(task.getZkpath(), task.convertJson());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		switch (TaskType.valueOf(task.getType())) {
		case AUTO:
			processLOG.info("update AUTODB  stat = "+task.getStat());

			break;

		case HTTP:
			processLOG.info("update HTTPDB   stat = "+task.getStat());
			break;

		case PERFORME:
			processLOG.info("update PERFORMEDB   stat = "+task.getStat());

			break;
		case INTERFACE:
			processLOG.info("update INTERFACEDB   stat = "+task.getStat());

			break;
		default:
			break;
		}
	}

	public static void opsucessTask(Map<String, String> data,ZkTask task) throws Exception{
		task.setStat(ZkTaskStatus.success.name());
		updateDB(task);
		for(String client:task.getClient()){
			String taskPath=ZKPaths.makePath(ZkTaskPath.getClientTaskPath(task.getType(), client), String.valueOf(task.getTaskid()));
			task.setStat(ZkTaskStatus.finish.name());
			ZkDataUtils.setData(taskPath, task.convertJson());
		}
	}
	public static void operatCliTask(Map<String, String> data,ZkTask task,String client){
		ZkTaskStatus stat1=ZkTaskStatus.valueOf(data.get(client));
		String taskPath=ZKPaths.makePath(ZkTaskPath.getClientTaskPath(task.getType(), client), String.valueOf(task.getTaskid()));
		try {
			if(!ZkDataUtils.isExists(taskPath)){
				processLOG.error("operatCliTask=" +taskPath+" is not exist!!");
				return;
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		switch (stat1) {
		case fail:
		case running:
		case stop:
			// to stop 
			processLOG.info("taskid: "+task.getTaskid()+" "+stat1.name()+" "+client+" should stop the task!!!");
			//client是否存活

			task.setStat(ZkTaskStatus.stop.name());
			try {
				ZkDataUtils.setData(taskPath, task.convertJson());
			} catch (Exception e) {
				processLOG.error(e.getMessage());
			}
			break;
		case create:
			System.out.println("++++++++ stat is creat to start ++++++++++");
			task.setStat(ZkTaskStatus.start.name());
			try {
				ZkDataUtils.setData(taskPath, task.convertJson());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				processLOG.error(e.getMessage());
				e.printStackTrace();
			}
			break;

		default:
			try {
				if(ZkDataUtils.isExists(taskPath)){
					System.out.println("check taskPath is exist !!!"+taskPath);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		}
	}

	public static void opfailTask(Map<String, String> data,ZkTask task){
		task.setStat(ZkTaskStatus.fail.name());
		updateDB(task);
		for(String client:task.getClient()){
			operatCliTask(data,task,client);
		}
	}
	public static boolean choiceClient2task(ZkTask zktask,ZkRegistMonitor registMonitor){
		return ZkTaskDistribution.choiceClient2task(zktask,registMonitor);
	}
}
