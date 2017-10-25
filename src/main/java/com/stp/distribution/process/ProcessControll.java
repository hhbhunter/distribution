package com.stp.distribution.process;
/**
 * 
 * @author hhbhunter
 *
 */
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import com.stp.distribution.framwork.ZkTaskPath;

public class ProcessControll {
	//auto & perform
	//task index
	private CuratorFramework zkinstance;
	private String type;
	private PathChildrenCache processCache;
	private static ProcessService pss= new ProcessService();
	private  boolean listenStat=false;
	
	public ProcessControll(CuratorFramework client,String type){
		zkinstance=client;
		this.type=type;
	}
	
	public boolean isListenStart() {
		return listenStat;
	}

	public void initProcessControll(){
		System.out.println(type +" process 监听开始");
		processCache=new PathChildrenCache(zkinstance, ZkTaskPath.getProcessPath(type), true);
		processCache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
					throws Exception {
				pss.processEvent(zkinstance,event);
				listenStat=true;
			}
		});
		try {
			processCache.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
