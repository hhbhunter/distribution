package com.stp.distribution.user;
/**
 * 
 * @author hhbhunter
 *
 */
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.process.ProcessTaskOperate;

public abstract class TaskResults {
	private static final Logger controLOG = LoggerFactory.getLogger(TaskResults.class);
	
	public abstract void updateDB(ZkTask task);
	
	public  void taskStatEvent(ZkTask task) throws Exception{
		TaskCache.updateTask(task);
		controLOG.info("result task id="+task.getTaskid()+" stat="+task.getStat());
		switch (ZkTaskStatus.valueOf(task.getStat())) {
		case stop:
			if(task.getClient().size()==0){
				TaskCache.delTask(task.getTaskid());
				break;
			}
			//已执行
			String processPath=ZKPaths.makePath(ZkTaskPath.getProcessPath(task.type),String.valueOf(task.getTaskid()));
			String indexPath=ZKPaths.makePath(ZkTaskPath.getProcessPath(task.type), ZkTaskPath.INDEX_PATH);
			String currentIndex=ZkDataUtils.getKVData(indexPath, ZkTaskPath.INDEX_PATH);
			if(!ProcessTaskOperate.currVsNewIndex(currentIndex,ZKPaths.getNodeFromPath(task.getZkpath()),task.getType())){
				Map<String,String> data=new HashMap<>();
				if(ZkDataUtils.isExists(processPath)){
					for(String client:task.getClient()){
						data.put(ZKPaths.makePath(ZkTaskPath.getClientTaskPath(task.getType(), String.valueOf(task.getTaskid())), client), task.convertJson());
					}
					ZkDataUtils.transaction(data);
				}
				
			}else{
				TaskCache.stopCache.put(task.getTaskid(), task);
				
			}
			break;
		case finish:
			TaskCache.delTask(task.getTaskid());
			break;
		case success:
		case fail:
			updateDB(task);
			break;

		default:
			break;
		}
	}
}
