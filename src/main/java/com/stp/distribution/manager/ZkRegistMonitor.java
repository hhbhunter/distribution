package com.stp.distribution.manager;
/**
 * 
 * @author hhbhunter
 *
 */
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import com.google.common.collect.Maps;
import com.stp.distribution.framwork.ZKConfig;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;

public class ZkRegistMonitor {
	private Map<String,Integer> registCache=Maps.newConcurrentMap();
	private CuratorFramework zkinstance;
	private String type;
	private PathChildrenCache registPathCache;
	
	public ZkRegistMonitor(CuratorFramework client,String type){
		zkinstance=client;
		this.type=type;
	}
	public void initRegistCache(){
		registPathCache=new PathChildrenCache(zkinstance, ZkTaskPath.getMonitorPath(type), true);
		registPathCache.getListenable().addListener(new PathChildrenCacheListener() {
			
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
					throws Exception {
				// TODO Auto-generated method stub
				monitorChildrenEvent(event);
			}
		});
		try {
			registPathCache.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@SuppressWarnings("unused")
	public void monitorChildrenEvent(PathChildrenCacheEvent event){
		String clientPath=event.getData().getPath();
		String clientIp=ZKPaths.getNodeFromPath(clientPath);
		String clientTypePath=ZkTaskPath.getClientTaskPath(type, clientIp);
		String data = null;
		try {
			
			data=new String(event.getData().getData(),ZKConfig.getZkCharset());
			
			if(data==null){
				data=ZkDataUtils.getData(clientTypePath);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		switch (event.getType()) {
		case CHILD_ADDED:
		case CHILD_UPDATED:
			if(data==null){
				registCache.put(clientIp, 0);
			}else{
				registCache.put(clientIp, Integer.valueOf(data));
			}
			System.out.println("regist:"+clientIp+" data:"+data);
			break;
		case CHILD_REMOVED:
			registCache.remove(clientIp);
			System.out.println("regist remove:"+clientIp+" data:"+data);
			break;
		default:
			break;
		}
		
	}
	public Map<String, Integer> getRegistCache() {
		return registCache;
	}
	
	public void updateRegistCache(List<String> clients,int raise) {
		for(String cli:clients){
			int num=registCache.get(cli);
			registCache.put(cli, num+raise);
		}
		
	}
	
	public void stopListen(){
		registPathCache.clear();
		try {
			registPathCache.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
