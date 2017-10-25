package com.stp.distribution.client;

import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.stp.distribution.entity.TaskType;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.util.UtilTool;

public class ZkClientControll {
	private static final Logger clientLOG = LoggerFactory.getLogger(ZkClientControll.class);
	private static CuratorFramework zkInstance = null;

	private static String myClientPath;

	private static String myip=UtilTool.getLocalIp();

	public static Map<Integer,ZkTask> autoMap=Maps.newConcurrentMap();

	public static Map<Integer,ZkTask> performMap=Maps.newConcurrentMap();

	private ZkClientTask clientTaskExe;

	private PathChildrenCache autoChildrenCache;
	private PathChildrenCache performChildrenCache;
	private String autoPath;
	private String performPath;
	
	private NodeCache autoRegistNode;
	private NodeCache performRegistNode;

	private PathChildrenCacheListener autoTaskListener=new PathChildrenCacheListener() {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
				throws Exception {
			// TODO Auto-generated method stub
			clientTaskExe.taskProcess(event, autoMap,myip);

		}
	};

	private PathChildrenCacheListener performTaskListener=new PathChildrenCacheListener() {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
				throws Exception {
			// TODO Auto-generated method stub
			clientTaskExe.taskProcess(event, performMap,myip);
		}
	};

	public ZkClientControll(CuratorFramework client, String clientPath){
		myClientPath=clientPath;
		zkInstance=client;
		clientTaskExe=new ZkClientTask(zkInstance);
		autoPath=ZKPaths.makePath(myClientPath, TaskType.AUTO.name());
		performPath=ZKPaths.makePath(myClientPath, TaskType.PERFORME.name());
		autoChildrenCache=new PathChildrenCache(client, autoPath, true);
		performChildrenCache=new PathChildrenCache(client, performPath, true);

	}

	public void initClient(){
		if(creatClientPath()){
			clientLOG.info("client path creat ok !!");
			initAllListen();
		}else{
			clientLOG.error("client path creat failed !!");
		}
		
	}
	/**
	 * 可扩展
	 * @param type
	 */
	public void  checkAllTask(String type){
		String typePath=ZkTaskPath.getClientTaskPath(type, myip);
		String typeProcessPath=ZkTaskPath.getProcessPath(type);
		try {
			List<String> childrens=ZkDataUtils.getChildren(typePath);
			for(String taskid:childrens){
				//
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public boolean creatClientPath(){
		System.out.println("create path=="+ZKPaths.makePath(myClientPath, TaskType.AUTO.name()));
		clientLOG.debug("create path=="+ZKPaths.makePath(myClientPath, TaskType.AUTO.name()));
		boolean flag=false;
		try {
			if(!ZkDataUtils.isExists(myClientPath)){
				ZkDataUtils.createDataPath(myClientPath, "cli");
				ZkDataUtils.createDataPath(ZKPaths.makePath(myClientPath, TaskType.AUTO.name()), "0");
				ZkDataUtils.createDataPath(ZKPaths.makePath(myClientPath, TaskType.PERFORME.name()), "0");
//				ZkDataUtils.createDataPath(ZKPaths.makePath(myClientPath, ZkTaskPath.NODE_THREAD),"0");
			}
			if(!ZkDataUtils.isExists(myClientPath)){
				clientLOG.error(myClientPath +" path create failed !!!");
			}else{
				flag=true;
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
	}

	public void initAllListen(){
		autoChildrenCache.getListenable().addListener(autoTaskListener);
		performChildrenCache.getListenable().addListener(performTaskListener);
		try {
			autoChildrenCache.start();
			performChildrenCache.start();
			initClientLive();
			autoRegistNode=registClient(TaskType.AUTO.name());
			performRegistNode=registClient(TaskType.PERFORME.name());
			clientLOG.info("client listener is init ok !!");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void initClientLive(){
		final String livePath=ZKPaths.makePath(myClientPath, "live");
		try {
			if(!ZkDataUtils.isExists(livePath)){
				zkInstance.create().withMode(CreateMode.EPHEMERAL).forPath(livePath);
			}
			if(!ZkDataUtils.isExists(livePath)){
				System.out.println(" creat livenode failed !!!");
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		final NodeCache liveNode = new NodeCache(zkInstance, livePath);
		NodeCacheListener nodeListener=new NodeCacheListener() {

			@Override
			public void nodeChanged() throws Exception {
				if(!ZkDataUtils.isExists(livePath)){
					ZkDataUtils.createEPHEMERALDataPath(livePath, "0");
				}
				System.out.println("nodeChanged=="+liveNode.getCurrentData().getPath()+" stat=="+liveNode.getCurrentData().getStat());
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
	public NodeCache registClient(String type){
		final String registPath=ZKPaths.makePath(ZkTaskPath.getMonitorPath(type), myip);
		final String typePath=ZKPaths.makePath(myClientPath, type);
		try {
			if(!ZkDataUtils.isExists(registPath)){
				int size=ZkDataUtils.getChildren(typePath).size();
				ZkDataUtils.createEPHEMERALDataPath(registPath, String.valueOf(size));
			}
			if(!ZkDataUtils.isExists(registPath)){
				System.out.println("registPath create failed !!!");
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		final NodeCache registNode = new NodeCache(zkInstance, registPath);
		NodeCacheListener nodeListener=new NodeCacheListener() {

			@Override
			public void nodeChanged() throws Exception {
				if(!ZkDataUtils.isExists(registPath)){
					int size=ZkDataUtils.getChildren(typePath).size();
					ZkDataUtils.createEPHEMERALDataPath(registPath, String.valueOf(size));
				}else{
					System.out.println("nodeChanged=="+registNode.getCurrentData().getPath()+" createTime=="+registNode.getCurrentData().getStat().getCtime());
				}
			}
		};
		registNode.getListenable().addListener(nodeListener);
		try {
			registNode.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return registNode;
	}
	
	public void clearRegist(String type){
		try{
		switch (TaskType.valueOf(type)) {
		case AUTO:
			autoRegistNode.close();
			break;
		case PERFORME:
			performRegistNode.close();
			break;

		default:
			break;
		}
		}catch(Exception e){
			e.printStackTrace();
		}
	}


}
