package com.stp.distribution.framwork;

import java.util.Collections;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import com.stp.distribution.entity.TaskType;

/**
 * 查询删除工具
 * @author houhuibin
 *
 */
public class NodeListen {
	
	public static void main(String[] args) throws Exception {
		ZKConfig.readConfig("config/zkclientconfig.properties");
		
		CuratorFramework zk=ZKinstance.getZKInstance();
		zk.start();
		ZkDataUtils.setZKinstance(zk);
		System.out.println("controll...");
		System.out.println(ZkDataUtils.getChildren(ZkTaskPath.CONTROLL_PATH));
		List<String> paths=ZkDataUtils.getChildren(ZkTaskPath.getControllPath(TaskType.AUTO.name()));
		Collections.sort(paths);
		System.out.println(paths);
		for(String path:ZkDataUtils.getChildren(ZkTaskPath.getControllPath(TaskType.AUTO.name()))){
			System.out.println(ZkDataUtils.getData(ZKPaths.makePath(ZkTaskPath.getControllPath(TaskType.AUTO.name()), path)));
		}
		
		System.out.println("process task");
		System.out.println(ZkDataUtils.getChildren(ZkTaskPath.PROCESS_PATH));
		System.out.println(ZkDataUtils.getChildren(ZkTaskPath.getProcessPath(TaskType.AUTO.name())));
		for(String path:ZkDataUtils.getChildren(ZkTaskPath.getProcessPath(TaskType.AUTO.name()))){
			System.out.println("path="+path);
			System.out.println(ZkDataUtils.getData(ZKPaths.makePath(ZkTaskPath.getProcessPath(TaskType.AUTO.name()), path)));
			
		}
		System.out.println("monitor...");
		System.out.println(ZkDataUtils.getChildren(ZkTaskPath.MONITOR_PATH));
		
		for(String path:ZkDataUtils.getChildren(ZkTaskPath.getMonitorPath(TaskType.AUTO.name()))){
			System.out.println(ZKPaths.makePath(ZkTaskPath.getMonitorPath(TaskType.AUTO.name()), path));
			System.out.println(ZkDataUtils.getData(ZKPaths.makePath(ZkTaskPath.getMonitorPath(TaskType.AUTO.name()), path)));
			System.out.println(path+".....");
			System.out.println(ZkDataUtils.getChildren(ZkTaskPath.getClientTaskPath(TaskType.AUTO.name(),path)));
			for(String task:ZkDataUtils.getChildren(ZkTaskPath.getClientTaskPath(TaskType.AUTO.name(),path))){
				System.out.println(ZkDataUtils.getData(ZKPaths.makePath(ZkTaskPath.getClientTaskPath(TaskType.AUTO.name(),path), task)));
			}
		}
		for(String path:ZkDataUtils.getChildren(ZkTaskPath.getMonitorPath(TaskType.PERFORME.name()))){
			System.out.println(ZKPaths.makePath(ZkTaskPath.getMonitorPath(TaskType.PERFORME.name()), path));
			System.out.println(ZkDataUtils.getData(ZKPaths.makePath(ZkTaskPath.getMonitorPath(TaskType.PERFORME.name()), path)));
			System.out.println(path+".....");
			System.out.println(ZkDataUtils.getChildren(ZkTaskPath.getClientTaskPath(TaskType.PERFORME.name(),path)));
			for(String task:ZkDataUtils.getChildren(ZkTaskPath.getClientTaskPath(TaskType.PERFORME.name(),path))){
				System.out.println(ZkDataUtils.getData(ZKPaths.makePath(ZkTaskPath.getClientTaskPath(TaskType.PERFORME.name(),path), task)));
			}
		}
		
		if(ZkDataUtils.isExists(ZkTaskPath.CONTROLL_PATH))
		ZkDataUtils.removeDataPathAndChildren(ZkTaskPath.CONTROLL_PATH);
		if(ZkDataUtils.isExists(ZkTaskPath.MONITOR_PATH))
			ZkDataUtils.removeDataPathAndChildren(ZkTaskPath.MONITOR_PATH);
		if(ZkDataUtils.isExists(ZkTaskPath.PROCESS_PATH))
			ZkDataUtils.removeDataPathAndChildren(ZkTaskPath.PROCESS_PATH);
		if(ZkDataUtils.isExists(ZkTaskPath.CLIENT_PATH))
			ZkDataUtils.removeDataPathAndChildren(ZkTaskPath.CLIENT_PATH);
		
	}

}
