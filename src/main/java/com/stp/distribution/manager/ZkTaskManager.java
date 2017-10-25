package com.stp.distribution.manager;
/**
 * @author houhuibin
 */
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.stp.distribution.entity.TaskType;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.framwork.IZkTaskOperat;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.process.ProcessControll;
import com.stp.distribution.process.ProcessHandler;
import com.stp.distribution.process.ProcessService;
import com.stp.distribution.process.ProcessTaskListen;
import com.stp.distribution.user.TaskCache;
import com.stp.distribution.user.ZkTaskControll;


//controll task 
public class ZkTaskManager {
	private static final Logger managerLOG = LoggerFactory.getLogger(ZkTaskManager.class);

	public static CuratorFramework zkInstance;
	public static IZkTaskOperat zkTaskOperat;

	public static ReentrantLock lock=new ReentrantLock();

	/*public static ZkTaskControll autoTaskControll;
	public static ZkTaskControll performTaskControll;*/

	public static ZkRegistMonitor autoClientsMonitor;
	public static ZkRegistMonitor performClientsMonitor;

	public static ProcessControll autoProcessControll;
	public static ProcessControll performProcessControll;

	public static boolean reboot=true;


	public void initAll(){
		zkInstance=ZkDataUtils.getZKinstance();
		try {
			ZkTaskPath.initOrgPath();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			managerLOG.error(e.getMessage());
			e.printStackTrace();
		}
		initProcess();
//		initControl();
		initRegistMonitor();
	}

	public void startTaskExecute(){
		while(!autoProcessControll.isListenStart() || ! performProcessControll.isListenStart()){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
			}
		}
		try {
			initProcessTaskListen();
//			new Thread(new ZkTaskOperatImp()).start();
			new Thread(new ProcessHandler(autoClientsMonitor,TaskType.AUTO.name())).start();
			new Thread(new ProcessHandler(performClientsMonitor,TaskType.PERFORME.name())).start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	/*void initControl(){
		autoTaskControll=new ZkTaskControll(zkInstance,TaskType.AUTO.name() );
		performTaskControll=new ZkTaskControll(zkInstance,TaskType.PERFORME.name() );
		autoTaskControll.initControll();
		performTaskControll.initControll();

	}*/

	void initRegistMonitor(){
		autoClientsMonitor=new ZkRegistMonitor(zkInstance, TaskType.AUTO.name());
		performClientsMonitor=new ZkRegistMonitor(zkInstance, TaskType.PERFORME.name());
		autoClientsMonitor.initRegistCache();
		performClientsMonitor.initRegistCache();
	}

	void initProcess(){
		autoProcessControll=new ProcessControll(zkInstance, TaskType.AUTO.name());
		performProcessControll=new ProcessControll(zkInstance, TaskType.PERFORME.name());
		autoProcessControll.initProcessControll();
		performProcessControll.initProcessControll();
	}
	/**
	 * 重载zkdata,保留
	 * @param path
	 * @throws Exception
	 */
	public void retryGetTaskQueue(String path) throws Exception{
		List<String> controlldata=ZkDataUtils.getChildren(path);
		if(controlldata.isEmpty())return;
		for(String children:controlldata){
			String type=ZKPaths.getNodeFromPath(children);
			switch (TaskType.valueOf(type)) {
			case AUTO:
			case PERFORME:
				String taskPath=ZkDataUtils.getKVData(ZkTaskPath.getProcessPath(type), ZkTaskPath.INDEX_PATH);
				getClidrensTask(children,taskPath);
				break;

			default:
				break;
			}

		}
	}
	private void getClidrensTask(String parentPath,String startPath) throws Exception{
		List<String> controlldata=ZkDataUtils.getChildren(parentPath);
		Collections.sort(controlldata);
		for(String children:controlldata.subList(controlldata.indexOf(startPath)+1, controlldata.size())){

			String jsondata=ZkDataUtils.getData(children);
			if(jsondata==null || jsondata.equals("")) continue;
			ZkTask zktask=JSON.parseObject(jsondata, ZkTask.class);
			zktask.setZkpath(children);
			TaskCache.addTask(zktask);
		}
	}
	//预分配
	public static boolean choiceClient2task(ZkTask zktask){
		switch (TaskType.valueOf(zktask.getType())) {
		case AUTO:
			return ZkTaskDistribution.choiceClient2task(zktask,autoClientsMonitor);
		case PERFORME:
			return ZkTaskDistribution.choiceClient2task(zktask,performClientsMonitor);
		default:
			break;
		}
		return false;
	}
	public static boolean reduceClient2task(ZkTask zktask){
		for(String client:zktask.getClient())
			switch (TaskType.valueOf(zktask.getType())) {
			case AUTO:
				autoClientsMonitor.getRegistCache().remove(client);
				return true;
			case PERFORME:
				performClientsMonitor.getRegistCache().remove(client);
				break;
			}
		return false;
	}

	public static void initProcessTaskListen() throws Exception{
		initAllProTaskListen(TaskType.AUTO.name());
		initAllProTaskListen(TaskType.PERFORME.name());

	}
	public static void initAllProTaskListen(String type) throws Exception{
		//		ProcessService ps=new ProcessService();
		String path=ZkTaskPath.getProcessPath(type);
		List<String> clidrens=ZkDataUtils.getChildren(path);
		clidrens.remove("current");
		for(String taskid:clidrens){
			ProcessTaskListen processTaskListen=new ProcessTaskListen(zkInstance);
			processTaskListen.startProcessTaskListen(taskid,type);
			ProcessService.taskListen.put(Integer.parseInt(taskid), processTaskListen);
			//			Map<String,String> data=ZkDataUtils.getMapData(ZKPaths.makePath(path, taskid));
			//			ZkTask task=ProcessTaskOperate.getTaskByPath(data.get(ProcessKey.SRC));
			//			ps.collectStat(data, task);
			//			ps.updateNodeData(data, task, ps.taskStats.get(task.getTaskid()));

		}
		Thread.sleep(1000);
	}

}
