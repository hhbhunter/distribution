package stp.roletest;
import java.util.List;
import java.util.Map;

import com.stp.distribution.entity.TaskType;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZKConfig;
import com.stp.distribution.user.TaskCreaterService;
import com.stp.distribution.user.TaskResults;
import com.stp.distribution.util.CmdExec;

public class DistributeTaskService extends TaskResults{
	static TaskCreaterService performTask;
	static {
		ZKConfig.readConfig("/zkclientconfig.properties");
	}

	public DistributeTaskService(){
		if(performTask==null){
			performTask=new TaskCreaterService(this);
			performTask.init();
		}
	}
	/**
	 * 同步文件，本地ip不同步
	 */
	@Override
	public boolean scpFile(ZkTask task){
		int sucNum=0;
		for(String client:task.getClient()){
			String scpCmd= "echo hello ";
			int stat=-1;
			if(scpCmd!=null){
				CmdExec exe=new CmdExec();
				stat=exe.cmdExec(scpCmd, true, true);
				if(stat==0)sucNum++;
			}else{
				sucNum++;
			}
		}
		if(sucNum==task.getClient().size()){
			return true;
		}
		return false;

	}
	/**
	 * 请实现状态数据库更新逻辑
	 */
	@Override
	public void updateDB(ZkTask task) {
		String planId=task.getTaskid();
		int executionId =-1;
		String taskType = "";
		if(planId.contains("_")){
			taskType = planId.split("_")[0];
			executionId=Integer.parseInt(planId.split("_")[1]);
		}else{
			executionId=Integer.parseInt(planId);
		}

		switch (ZkTaskStatus.valueOf(task.getStat())) {
		case stop:
		case success:
			//执行完成

			break;
		case fail:
			//执行失败

			break;
		default:
			break;
		}

	}
	/**
	 * @param planName 测试计划文件名
	 * @param workPath	测试计划存放目录
	 * @param planId	测试计划http_id或java_id
	 */
	public void addPeformTask(String planName,String workPath,String planId,List<String> clients){
		ZkTask task=new ZkTask();
		task.setCmdPath(workPath);
		task.setTaskid(planId);
		task.setClient(clients);
		task.setStartCmd("echo my is start....");
		task.setStopCmd("echo stop testing....");
		task.setExeNum(1);
		task.setStat(ZkTaskStatus.check.name());
		task.setType(TaskType.PERFORME.name());
		performTask.addTask(task);
	}
	/**
	 * 
	 * @param planId
	 */
	public void stopPerformTask(String planId){

		performTask.stopTask(planId);
	}

	public static Map<String, String> getLog(String taskId){
		return performTask.getTaskLog(taskId);
	}

	public static List<String> getAllExeClients() throws Exception{
		return performTask.getClients(TaskType.PERFORME.name());
	}

}
