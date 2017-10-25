package stp.roletest;

import java.util.concurrent.TimeUnit;

import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.manager.ZkTaskManager;
import com.stp.distribution.user.TaskCache;
import com.stp.distribution.user.TaskCreaterService;
import com.stp.distribution.user.TaskResults;


public class ZkTaskStartMain {
	public static void main(String[] args) {
		ZkTaskManager taskManager=new ZkTaskManager();
		taskManager.initAll();
		taskManager.startTaskExecute();
		
		while(true){
			try {
				TimeUnit.MINUTES.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
