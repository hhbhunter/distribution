package com.stp.distribution.client;
/**
 * @author houhuibin
 */

import com.stp.distribution.framwork.ZKConfig;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.manager.ZkTaskManager;
import com.stp.distribution.util.UtilTool;

public class ClientStart {
	private static void init(String myclientpath){
		
		ZkClientControll zkcli=new ZkClientControll(ZkDataUtils.getZKinstance(), myclientpath);
		zkcli.initClient();
	}
	
	private static void startClient(){
		String myclientpath=ZkTaskPath.getClientPath(UtilTool.getLocalIp());
		init(myclientpath);
	}
	
	private static void startControll(){
		ZkTaskManager taskManager=new ZkTaskManager();
		taskManager.initAll();
		taskManager.startTaskExecute();
	}
	
	public static void main(String[] args) {
		if(ZKConfig.getRole().equalsIgnoreCase("client")){
			startClient();
		}else{
			startControll();
		}
		
		while(true){
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
			}
		}
	}

}
