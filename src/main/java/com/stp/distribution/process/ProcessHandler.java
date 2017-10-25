package com.stp.distribution.process;
/**
 * 
 * @author hhbhunter
 *
 */
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkTaskPath;
import com.stp.distribution.manager.ZkRegistMonitor;



public class ProcessHandler implements Runnable {//Callable<String>
	private static final Logger processLOG = LoggerFactory.getLogger(ProcessHandler.class);
	private String type;
	private ZkRegistMonitor registMonitor;
	private String currTaskPath;

	public ProcessHandler(ZkRegistMonitor registMonitor,String type){
		this.registMonitor=registMonitor;
		this.type=type;
	}
	public String getCurrTaskPath(){
		return currTaskPath;
	}
	@Override
	public void run() {
		while(true){

			while( !checkCurrentIndex(processTaskHandler())){
				try {
					Thread.sleep(2000);
				} catch (Exception e) {
					// TODO Auto-generated catch block
				}
			}
		}
	}

	public String call() throws Exception {

//		String creatTaskPath=processTaskHandler();

		while(!checkCurrentIndex(processTaskHandler())){
			Thread.sleep(500);
		}
		return currTaskPath;
	}

	public boolean checkCurrentIndex(String newTaskPath){
		boolean flag=false;
		try {

			if(currTaskPath.equalsIgnoreCase(newTaskPath)){
				flag=true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
	}

	public String processTaskHandler(){
		try {
			currTaskPath=ProcessTaskOperate.getCurrentTaskIndex(type);
			processLOG.info(type + "current index=="+currTaskPath);
			return creatProcessTask(type,0l);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String creatProcessTask(String type,long newIndex){

		if(!currTaskPath.equals(ZkTaskPath.INDEX_PATH)){
			newIndex=Long.valueOf(currTaskPath.substring(type.length()))+1;
		}

		String newTaskPath=ProcessTaskOperate.genrateNewIndex(type, String.valueOf(newIndex));
		processLOG.info(type + " new processTask==="+newTaskPath);

		try {
			if(!ZkDataUtils.isExists(newTaskPath)){

				String controllTaskPath=ZKPaths.makePath(ZkTaskPath.getControllPath(type),newTaskPath);
				if(ZkDataUtils.isExists(controllTaskPath)){
					//尝试创建
					ZkTask myTask=null;
					try{
						myTask=ProcessTaskOperate.getTaskByPath(controllTaskPath);
					}catch(Exception e){
						processLOG.error(" 【json】 formate is error ,please check !!! \n"+myTask.convertJson() );
						//跳过错误id，处理
						creatProcessTask(type,newIndex+1);
					}
					if(myTask==null || myTask.getStat().equals(ZkTaskStatus.stop.name())){
						processLOG.info(controllTaskPath+"is null or stop !! "+myTask.convertJson());
						creatProcessTask(type,newIndex+1);
					}
					//create
					if(ProcessTaskOperate.createProcessTask(myTask)){
						processLOG.info("create process task id "+myTask.getTaskid()+" ok!! ");
						return newTaskPath;
					}
					
					
				}else{
					
					processLOG.error(controllTaskPath +" is not exist !!!");
					Thread.sleep(1000);
				}
			}else{
//				processLOG.info(type + " current processTask==="+newTaskPath);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
