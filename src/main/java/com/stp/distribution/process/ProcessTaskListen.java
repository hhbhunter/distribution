package com.stp.distribution.process;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stp.distribution.entity.ProcessKey;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;

public class ProcessTaskListen {
	private static final Logger processLOG = LoggerFactory.getLogger(ProcessTaskListen.class);
	public static boolean stat=true;
	public static String currentIndex;
	public static long  newIndex;
	private PathChildrenCache processTaskCache;
	private CuratorFramework zkinstance;
	public ProcessTaskListen(CuratorFramework client){
		this.zkinstance=client;
	}

	public void startProcessTaskListen(String taskid,String type) throws Exception{
		processTaskCache=new PathChildrenCache(zkinstance, ZKPaths.makePath(ZkTaskPath.getProcessPath(type), taskid), true);
		processTaskCache.getListenable().addListener(new PathChildrenCacheListener() {

			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
					throws Exception {
				// TODO Auto-generated method stub
				checkProcessChildren(event);
			}
		});
		processTaskCache.start();
	}
	void checkProcessChildren(PathChildrenCacheEvent event) throws Exception{

		switch (event.getType()) {
		case CHILD_ADDED:
		case CHILD_UPDATED:
			String childPath=event.getData().getPath();
			String stat=ZkDataUtils.getKVData(childPath,ProcessKey.STAT);
			processLOG.info("update stat==="+stat+" client"+childPath);
			String parentPath=ZKPaths.getPathAndNode(childPath).getPath();
			String client=ZKPaths.getNodeFromPath(childPath);
			if(ZkDataUtils.isExists(parentPath)){
				ZkDataUtils.setKVData(parentPath, client, stat);
			}else{
				processLOG.error(parentPath+ " is DELETE !!");
			}
			break;

		default:
			break;
		}

	}
	public void closeProcessTaskListen() throws IOException{
		processTaskCache.getListenable().clear();
		processTaskCache.clear();
		processTaskCache.close();
	}


}
