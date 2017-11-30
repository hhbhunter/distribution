package com.stp.distribution.process;

import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;

import com.google.common.collect.Maps;
import com.stp.distribution.entity.ProcessKey;
import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.manager.ZkTaskDistribution;
import com.stp.distribution.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author hhbhunter
 *
 */

public class ProcessService implements IProcess{

	private static final Logger processLOG = LoggerFactory.getLogger(ProcessService.class);
	private final static String INDEX_PATH=ZkTaskPath.INDEX_PATH;
	public static Map<String,ProcessTaskListen> taskListen=Maps.newConcurrentMap();
	public static Map<String,ZkTaskStatus> taskStats=Maps.newConcurrentMap();

	@Override
	public void createNodeData(Map<String, String> data,ZkTask task) {
		// TODO Auto-generated method stub

	}

	@Override
	public  void updateNodeData(Map<String, String> data,ZkTask task,ZkTaskStatus stat) {
		ProcessTaskOperate.updateNodeData(data,task,stat);
	}

	@Override
	public  boolean collectStat(Map<String, String> data,ZkTask task) {
		int size=0;
		int finish=0;
		for(String client:task.getClient()){
			ZkTaskStatus stat=ZkTaskStatus.valueOf(data.get(client));
			switch (stat) {
			case success:
				size++;
				break;
			case finish:
				finish++;
				break;
			default:

				if(taskStats.containsKey(task.getTaskid())){
					if(taskStats.get(task.getTaskid()).getPriority()<stat.getPriority()){
						taskStats.put(task.getTaskid(), stat);
					}else{
						processLOG.info(task.getTaskid() +" 任务状态没有变更");
						return false;
					}
				}else{
					taskStats.put(task.getTaskid(),stat);
				}

				break;
			}
		}
		if(size==task.getClient().size()){
			taskStats.put(task.getTaskid(),ZkTaskStatus.success);
		}
		if(finish==task.getClient().size()){
			taskStats.put(task.getTaskid(),ZkTaskStatus.finish);
		}
		return true;
	}

	public  void processEvent(CuratorFramework client,PathChildrenCacheEvent event) throws Exception{
		String dataPath=event.getData().getPath();
		processLOG.info("processEvent path == "+dataPath);
		String json=new String(event.getData().getData());
		String prePath=ZKPaths.getPathAndNode(dataPath).getPath();//{path,node}
		String taskid=ZKPaths.getPathAndNode(dataPath).getNode();//{path,node}
		String type=ZKPaths.getNodeFromPath(prePath);
		String indexPath=ZKPaths.makePath(prePath, INDEX_PATH);
		String currentIndex;
		Map<String,String> dataMap;
		ZkTask myTask;
		switch (event.getType()) {
		case CHILD_ADDED://process 新增节点比较taskindex
			if(indexPath.equals(dataPath) || taskid.equals(ProcessKey.TMP_PATH))break;
			currentIndex=ZkDataUtils.getKVData(indexPath, INDEX_PATH);
			processLOG.info("process 节点新增 :"+dataPath+" 当前index:"+currentIndex);
			dataMap=StringUtils.getStrMapByJSON(json);
			myTask=ProcessTaskOperate.getTaskByPath(dataMap.get(ProcessKey.SRC));
			processLOG.debug("process task===\n"+myTask.convertJson());

			/*直接分发执行*/
			switch (ZkTaskStatus.valueOf(myTask.getStat())) {
			case create:
				myTask.setStat(ZkTaskStatus.start.name());
				break;
			default:
				break;
			}

			if(currentIndex==null || currentIndex.equalsIgnoreCase(INDEX_PATH)){
				ZkDataUtils.setKVData(indexPath, INDEX_PATH, ZKPaths.getNodeFromPath(dataMap.get(ProcessKey.SRC)));//更新最新任务编号
			}else if(ProcessTaskOperate.currVsNewIndex(currentIndex,ZKPaths.getNodeFromPath(dataMap.get(ProcessKey.SRC)),type)){
				ZkDataUtils.setKVData(indexPath, INDEX_PATH, ZKPaths.getNodeFromPath(dataMap.get(ProcessKey.SRC)));//更新最新任务编号

			}
			if(taskListen.containsKey(myTask.getTaskid())){

				processLOG.error(myTask.getTaskid() +" is running please waiting !!!");

				break;
			}
			try{

				ZkTaskDistribution.distributTask(myTask);

				ProcessTaskListen processTask= new ProcessTaskListen(client);

				taskListen.put(myTask.getTaskid(), processTask);
				processTask.startProcessTaskListen(String.valueOf(myTask.getTaskid()), myTask.getType());

			}catch(Exception e){
				processLOG.error("distributTask failed !!!"+e.getMessage());
				e.printStackTrace();
				//当前会直接跳过已存在任务id，避免同一个任务在执行期内重复提交
				collectStat(dataMap,myTask);
				updateNodeData(dataMap,myTask,taskStats.get(taskid));

			}
			processLOG.debug(" update currentIndex == "+ZkDataUtils.getKVData(indexPath, INDEX_PATH));
			break;
		case CHILD_UPDATED:
			processLOG.info(" process CHILD_UPDATED !!! "+dataPath);
			if(dataPath.equalsIgnoreCase(indexPath)){
				System.out.println("跳过");
				break;
			}
			// 任务状态变更
			dataMap=StringUtils.getStrMapByJSON(json);
			myTask=ProcessTaskOperate.getTaskByPath(dataMap.get(ProcessKey.SRC));
			collectStat(dataMap,myTask);

			//process去删除client任务，而不update数据（没有考虑分布式锁的问题）
			updateNodeData(dataMap,myTask,taskStats.get(taskid));

			break;
		case CHILD_REMOVED:
			//任务移除监听，拿最新任务尝试创建
			//此处是否删除control task_sequ
			processLOG.info("process  CHILD_REMOVED taskPath == "+dataPath);
			if(taskListen.containsKey(taskid))
			taskListen.get(taskid).closeProcessTaskListen();
			taskStats.remove(taskid);//删除任务状态
			taskListen.remove(taskid);
			dataMap=StringUtils.getStrMapByJSON(json);
			myTask=ProcessTaskOperate.getTaskByPath(dataMap.get(ProcessKey.SRC));
			myTask.setStat(ZkTaskStatus.finish.name());
			ZkDataUtils.setData(dataMap.get(ProcessKey.SRC), myTask.convertJson());
			break;
		default:
			break;
		}

	}
	//自动分配不需要轮询
	public static boolean creatTask(String type,long newIndex){

		processLOG.info("process newIndex = "+newIndex);
		String controllTaskPath=ZKPaths.makePath(ZkTaskPath.getControllPath(type),ProcessTaskOperate.genrateNewIndex(type, String.valueOf(newIndex)));
		processLOG.info("process controllTaskPath == "+controllTaskPath);
		boolean flag=false;
		try {
			if(ZkDataUtils.isExists(controllTaskPath)){
				//尝试创建
				ZkTask myTask=null;
				try{
					myTask=ProcessTaskOperate.getTaskByPath(controllTaskPath);
				}catch(Exception e){
					processLOG.error(" json formate is error ,please check !!! \n"+e.getMessage() );
					//跳过错误id，处理
					creatTask(type,newIndex+1);

				}
				if(myTask==null || myTask.getStat().equals(ZkTaskStatus.stop.name())){
					processLOG.info(controllTaskPath+"is null or stop !!");
				}
				//create
				if(ProcessTaskOperate.createProcessTask(myTask)){
					processLOG.info("create process task id "+myTask.getTaskid()+" ok!! ");
					flag= true;
				}


			}else{

				processLOG.error(controllTaskPath +" is not exist !!!");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
	}
	/*public static boolean creatTask(String controllTaskPath){
		boolean flag=false;
		try {
			if(ZkDataUtils.isExists(controllTaskPath)){
				//尝试创建
				ZkTask myTask=null;
				try{
					myTask=ProcessTaskOperate.getTaskByPath(controllTaskPath);
				}catch(Exception e){
					processLOG.error(" json formate is error ,please check !!! \n"+e.getMessage() );
					//跳过错误id，处理

				}
				if(myTask==null || myTask.getStat().equals(ZkTaskStatus.stop.name())){
					processLOG.info(controllTaskPath+"is null or stop !!");
				}
				//create
				if(ProcessTaskOperate.createProcessTask(myTask)){
					processLOG.info("create process task id "+myTask.getTaskid()+" ok!! ");
					flag= true;
				}


			}else{

				processLOG.error(controllTaskPath +" is not exist !!!");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
	}*/


}
