package stp.roletest;

import java.util.concurrent.TimeUnit;

import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.user.TaskCache;
import com.stp.distribution.user.TaskCreaterService;
import com.stp.distribution.user.TaskResults;

public class StartControl {
	public static void main(String[] args) {
		new TaskCreaterService(new TaskResults() {

			@Override
			public void updateDB(ZkTask task) {
				// TODO Auto-generated method stub
				System.out.println("control update task id="+task.getTaskid()+" stat="+task.getStat());
			}

			@Override
			public boolean scpFile(ZkTask task) {
				// TODO Auto-generated method stub
				return true;
			}
		}).init();
		String id="1";
		while(true){
			try {
				ZkTask task=new ZkTask();
				task.setStartCmd("you start cmd");
				task.setStopCmd("you stop cmd");
				task.setCmdPath("you work path");
				task.setExeNum(1);
				task.setStat("create");
				task.setTaskid(id);
				task.setType("PERFORME");
				//				task.setType("AUTO");
				TaskCache.addTask(task);
				TimeUnit.MINUTES.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
