package com.stp.distribution.client;
/**
 * @author hhbhunter
 */

import com.stp.distribution.framwork.ZKConfig;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.manager.ZkTaskManager;

public class ClientStart {
	
	private static void startClient(){
		ZkClientControll zkcli=new ZkClientControll(ZkDataUtils.getZKinstance());
		/* ｃｌｉｅｎｔ　重启需要对已有任务进行加载*/
		zkcli.retrieveTask();
		
		zkcli.initClient();
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
