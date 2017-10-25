package com.stp.distribution.framwork;
/**
 * 
 * @author hhbhunter
 *
 */
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import com.stp.distribution.entity.TaskType;

public class ZkTaskPath {

	public static String TASK_PATH="task";
	public static String CLIENT_PATH="stpclient";
	public static String CMD_PATH="cmd";
	public static String NODE_THREAD="threadNum";//节点线程数
	public static String CONTROLL_PATH="stpcontroll";//控制中心任务
	public static String MONITOR_PATH="stpmonitor";//监听
	public static String  PROCESS_PATH="stpprocess";//执行队列
	public static String SOURCE_PATH="stpresource";
	public static String INDEX_PATH="current";

	public static String getResourcePath(String ip){
		return ZKPaths.makePath(SOURCE_PATH, ip);
	}
	public static String getClientTaskPath(String type,String client){
		return ZKPaths.makePath(ZKPaths.makePath(CLIENT_PATH, client),type);
	}
	
	public static String getClientPath(String client){
		return ZKPaths.makePath(CLIENT_PATH, client);
	}
	
	public static String getControllPath(String type){
		return ZKPaths.makePath(CONTROLL_PATH, type);
	}

	public static String getProcessPath(String type){
		return ZKPaths.makePath(PROCESS_PATH, type);
	}

	public static String getMonitorPath(String type){
		return ZKPaths.makePath(MONITOR_PATH, type);
	}

	public static void initOrgPath() throws Exception{
		String processPath=getProcessPath(TaskType.AUTO.name());
		//resourcePath
		if(!ZkDataUtils.isExists(SOURCE_PATH)){
			ZkDataUtils.creatPersPath(SOURCE_PATH);
		}
		//process 
		if(!ZkDataUtils.isExists(processPath)){
			ZkDataUtils.createDataPath(processPath,TaskType.AUTO.name());
			ZkDataUtils.createDataPath(ZKPaths.makePath(processPath,INDEX_PATH),"{\""+INDEX_PATH+"\":\""+INDEX_PATH+"\"}");

		}
		processPath=getProcessPath(TaskType.PERFORME.name());
		if(!ZkDataUtils.isExists(processPath)){
			ZkDataUtils.createDataPath(processPath,TaskType.PERFORME.name());
			ZkDataUtils.createDataPath(ZKPaths.makePath(processPath,INDEX_PATH),"{\""+INDEX_PATH+"\":\""+INDEX_PATH+"\"}");
		}
		//controll center
		if(!ZkDataUtils.isExists(getControllPath(TaskType.AUTO.name()))){
			ZkDataUtils.createDataPath(getControllPath(TaskType.AUTO.name()), TaskType.AUTO.name());
		}
		if(!ZkDataUtils.isExists(getControllPath(TaskType.PERFORME.name()))){
			ZkDataUtils.createDataPath(getControllPath(TaskType.PERFORME.name()), TaskType.PERFORME.name());
		}

		// client register
		if(!ZkDataUtils.isExists(CLIENT_PATH)){
			ZkDataUtils.createDataPath(CLIENT_PATH, "\"cli\"");
		}

		// client collection
		if(!ZkDataUtils.isExists(getMonitorPath(TaskType.AUTO.name()))){
			ZkDataUtils.createDataPath(getMonitorPath(TaskType.AUTO.name()), TaskType.AUTO.name());
		}

		if(!ZkDataUtils.isExists(getMonitorPath(TaskType.PERFORME.name()))){
			ZkDataUtils.createDataPath(getMonitorPath(TaskType.PERFORME.name()), TaskType.PERFORME.name());
		}


	}


}
