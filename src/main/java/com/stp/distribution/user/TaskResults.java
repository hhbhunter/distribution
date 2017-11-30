package com.stp.distribution.user;

import java.util.HashMap;
import java.util.Map;

import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.stp.distribution.entity.ProcessKey;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.process.ProcessTaskOperate;
/**
 * 
 * @author hhbhunter
 *
 */
public abstract class TaskResults {
	private static final Logger controLOG = LoggerFactory.getLogger(TaskResults.class);
	/**
	 * 更新业务状态
	 * @param task
	 */
	public abstract void updateDB(ZkTask task);
	/**
	 * check状态做文件处理或校验
	 * @param task
	 * @return
	 */
	public abstract boolean scpFile(ZkTask task);
	
	public  void taskStatEvent(ZkTask task) throws Exception{
		TaskCache.updateTask(task);
		controLOG.info("result task id="+task.getTaskid()+" stat="+task.getStat());
		switch (ZkTaskStatus.valueOf(task.getStat())) {
		case create:
			if(task.getClient().size()>0){
				if(!scpFile(task)){
					task.setStat(ZkTaskStatus.fail.name());
				}
				String processPath=ZKPaths.makePath(ZkTaskPath.getProcessPath(task.getType()), String.valueOf(task.getTaskid()));
				if(ZkDataUtils.isExists(processPath)){
					Map<String,String> data=new HashMap<>();
					data.put(ProcessKey.SRC, task.getZkpath());
					for(String client:task.getClient()){
						data.put(client, task.getStat());
					}
					ZkDataUtils.setData(processPath, JSON.toJSONString(data));
				}else{
					controLOG.error("task id="+task.getTaskid()+"not has create processPath="+processPath);
				}
				
			}else{
				controLOG.error("task id="+task.getTaskid()+" not has exe client ip");
			}
			
			break;
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
						data.put(ZKPaths.makePath(ZkTaskPath.getClientTaskPath(task.getType(), client),  String.valueOf(task.getTaskid())), task.convertJson());
					}
					ZkDataUtils.updateTransaction(data);
				}
				
			}else{
//				TaskCache.stopCache.put(task.getTaskid(), task);
				
			}
			updateDB(task);
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
