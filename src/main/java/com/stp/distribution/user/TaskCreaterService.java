package com.stp.distribution.user;
/**
 * 
 * @author hhbhunter
 * 提供addTask、stopTask
 *
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stp.distribution.entity.ProcessKey;
import com.stp.distribution.entity.TaskType;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.process.ProcessTaskOperate;
import com.stp.distribution.util.UtilTool;


public class TaskCreaterService {
	private static final Logger taskLOG = LoggerFactory.getLogger(TaskCreaterService.class);
	public static boolean first=true;
	static ZkTaskOperatImp taskOperat=new ZkTaskOperatImp();
	public static ZkTaskControll autoTaskControll;
	public static ZkTaskControll performTaskControll;
	static TaskResults taskRes;
	// if you use it in web should init zkdatautil.class
	public TaskCreaterService(TaskResults taskRes){
		this.taskRes=taskRes;
		synchronized (TaskCreaterService.class) {
			if(first){
//				init();//不推荐的方式
				first=false;
			}
		}
		
	}
	
	//需提前初始化
	public void init(){
		List<String> resources=new ArrayList<String>();
		try {
			//临时处理，保证一个user具有执行权限
			resources = ZkDataUtils.getChildren(ZkTaskPath.SOURCE_PATH);
			taskLOG.info("ZkTaskPath.SOURCE_PATH size=="+resources.size());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(resources.isEmpty()){
			initTaskResource();
			initControl();
		}
		
		new Thread(taskOperat).start();
	}
	private static void initControl(){
		autoTaskControll=new ZkTaskControll(ZkDataUtils.getZKinstance(),TaskType.AUTO.name(),taskRes );
		performTaskControll=new ZkTaskControll(ZkDataUtils.getZKinstance(),TaskType.PERFORME.name(),taskRes);
		autoTaskControll.initControll();
		performTaskControll.initControll();
	}
	
	public  void addTask(ZkTask task){
		taskOperat.addTask(task);
	}
	
	public  void stopTask(String taskId){
		ZkTask task=getExeTask(taskId);
		boolean flag=false;
		if(task!=null){
			flag=taskOperat.stopTask(task);
		}
		if(!flag){
			//not exist exe task  update status stop
			task=new ZkTask();
			task.setTaskid(taskId);
			task.setType(TaskType.PERFORME.name());
			task.setStat(ZkTaskStatus.stop.name());
			taskRes.updateDB(task);
		}
		
	}
	/**
	 * 获取已注册client集合
	 * @param type
	 * @return
	 * @throws Exception
	 */
	public List<String> getClients(String type) throws Exception{
		return ZkDataUtils.getChildren(ZkTaskPath.getMonitorPath(type));
	}
	/**
	 * return clientip and tasklog
	 * @param taskId
	 * @return Map
	 */
	public Map<String,String> getTaskLog(String taskId){
		Map<String,String> logs=new HashMap<String,String>();
		ZkTask task=getExeTask(taskId);
		if(task!=null&&task.getType().equals(TaskType.PERFORME.name())){
			for(String client:task.getClient()){
				String logPath=ZKPaths.makePath(ZkTaskPath.getClientPath(client),"log");
				String taskLog=ZKPaths.makePath(logPath, taskId);
				try {
					if(ZkDataUtils.isExists(taskLog)){
						logs.put(client, ZkDataUtils.getData(taskLog));
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return logs;
	}
	private ZkTask getExeTask(String taskId){
		ZkTask task=null;
		if(TaskCache.taskCache.containsKey(taskId)){
			task=TaskCache.taskCache.get(taskId);
		}else{
			String procPath=ZKPaths.makePath(ZkTaskPath.getProcessPath(TaskType.PERFORME.name()),String.valueOf(taskId));
			try {
				if(ZkDataUtils.isExists(procPath)){
					String contrPath=ZkDataUtils.getMapData(procPath).get(ProcessKey.SRC);
					task=ProcessTaskOperate.getTaskByPath(contrPath);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return task;
	}
	
	private  void initTaskResource(){
		String ip=UtilTool.getLocalIp();
		
		final String resourcePath=ZkTaskPath.getResourcePath(ip);
		try {
//			String sourceip=ZkDataUtils.getData(ZkTaskPath.SOURCE_PATH);
//			if(sourceip.equals("")||sourceip.equals(null)){
//				
//				ZkDataUtils.setData(ZkTaskPath.SOURCE_PATH, ip);
//			}
			if(!ZkDataUtils.isExists(resourcePath)){
				ZkDataUtils.getZKinstance().create().withMode(CreateMode.EPHEMERAL).forPath(resourcePath);
			}
			if(!ZkDataUtils.isExists(resourcePath)){
				taskLOG.info(" creat resourcePath failed !!!");
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			taskLOG.error(e1.getMessage());
		}
		final NodeCache liveNode = new NodeCache(ZkDataUtils.getZKinstance(), resourcePath);
		NodeCacheListener nodeListener=new NodeCacheListener() {

			@Override
			public void nodeChanged() throws Exception {
				if(!ZkDataUtils.isExists(resourcePath)){
					ZkDataUtils.createEPHEMERALDataPath(resourcePath,"res");
					System.out.println("nodeChanged=="+liveNode.getCurrentData().getPath()+" stat=="+liveNode.getCurrentData().getStat());
				}
			}
		};
		liveNode.getListenable().addListener(nodeListener);
		try {
			liveNode.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
