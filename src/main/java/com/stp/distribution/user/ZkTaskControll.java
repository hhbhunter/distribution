package com.stp.distribution.user;

/**
 * 
 * @author hhbhunter
 *
 */
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;


public class ZkTaskControll {
	private static final Logger controLOG = LoggerFactory.getLogger(ZkTaskControll.class);
	private CuratorFramework zkInstance = null;
	private String type;
	private PathChildrenCache controllCache;
	private TaskResults taskRes;
	private PathChildrenCacheListener taskListener=new PathChildrenCacheListener() {

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
				throws Exception {
			contorllTaskEvent(event);
		}
	};
	public ZkTaskControll(CuratorFramework client,String type,TaskResults taskRes){
		zkInstance=client;
		this.type=type;
		this.taskRes=taskRes;
	}
	public void initControll(){
		if(creatControllPath()){
			controLOG.info("Controll path creat ok !!");
			initAllListen();
		}
	}
	public boolean creatControllPath(){
		String controllPath=ZkTaskPath.getControllPath(type);
		try {
			if(!ZkDataUtils.isExists(controllPath)){
				ZkDataUtils.createDataPath(controllPath, type+"con");
			}
			if(!ZkDataUtils.isExists(controllPath)){
				controLOG.error(controllPath+" controll path create failed !!!");
			}else{
				return true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			controLOG.error(e.getMessage());
		}
		return false;
	}
	
	public void initAllListen(){
		controllCache=new PathChildrenCache(zkInstance, ZkTaskPath.getControllPath(type), true);
		controllCache.getListenable().addListener(taskListener);
		try {
			controllCache.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			controLOG.error(e.getMessage());
		}
	
	}


	private void contorllTaskEvent(PathChildrenCacheEvent event){
		switch (event.getType()) {
		case CHILD_UPDATED:
			String task=new String(event.getData().getData());
			try {
				taskRes.taskStatEvent(JSON.parseObject(task, ZkTask.class));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;

		default:
			break;
		}
	}
	
	
	
}
