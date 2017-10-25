package com.stp.distribution.manager;

import java.util.concurrent.TimeUnit;

import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.user.TaskCache;
import com.stp.distribution.user.TaskCreaterService;
import com.stp.distribution.user.TaskResults;


public class ZkTaskStartMain {
	public static void main(String[] args) {
		ZkTaskManager taskManager=new ZkTaskManager();
		taskManager.initAll();
		taskManager.startTaskExecute();
		
		new TaskCreaterService(new TaskResults() {
			
			@Override
			public void updateDB(ZkTask task) {
				// TODO Auto-generated method stub
				System.out.println("control update task id="+task.getTaskid()+" stat="+task.getStat());
			}
		}).init();
		int id=1;
		while(true){
			try {
				ZkTask task=new ZkTask();
				task.setStartCmd("sh /opt/stpAgent/jmeter/bin/jmeter -n -t /opt/stpAgent/performWork/test/java_request1.jmx -A 1111");
				task.setStopCmd("cat /opt/stpAgent/performWork/test/1111.pid | xargs kill");
				task.setCmdPath("/opt/stpAgent/jmeter/bin/");
				task.setExeNum(1);
				task.setStat("create");
				task.setTaskid(id);
				task.setType("PERFORME");
//				task.setType("AUTO");
				TaskCache.addTask(task);
				TimeUnit.MINUTES.sleep(10);
				Thread.sleep(1000000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
