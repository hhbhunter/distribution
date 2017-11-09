package com.stp.distribution.user;
/**
 * 
 * @author hhbhunter
 * 提供addTask、stopTask
 *
 */
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stp.distribution.entity.TaskType;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
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
		synchronized (this) {
			if(first){
				init();//不推荐的方式
				first=false;
			}
		}
		
	}
	
	//需提前初始化
	public void init(){
		initTaskResource();
		initControl();
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
	
	public  void stopTask(int taskId){
		if(TaskCache.taskCache.containsKey(taskId)){
			taskOperat.stopTask(TaskCache.taskCache.get(taskId));
		}
	}
	
	private  void initTaskResource(){
		final String resourcePath=ZkTaskPath.getResourcePath(UtilTool.getLocalIp());
		try {
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
