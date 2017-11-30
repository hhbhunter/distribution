package com.stp.distribution.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stp.distribution.entity.ZkTask;
import com.stp.distribution.entity.ZkTaskStatus;
import com.stp.distribution.framwork.ZKConfig;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.framwork.ZkException;
import com.stp.distribution.framwork.ZkTaskPath;

public class ZkTaskDistribution {

	private static final Logger distrubtLOG = LoggerFactory.getLogger(ZkTaskDistribution.class);

	void distributPipline(){

	}

	void distributNodeConf(){

	}

	/**
	 * distribute client task
	 * @param task
	 * @throws Exception
	 */
	public void zkTaskEvent(ZkTask task) throws Exception {
		switch (ZkTaskStatus.valueOf(task.getStat())) {
		case create:
			//client create path
		case stop:
			//client task stat update
		case pause:
		case pending:
		case finish:
			distributTask(task);
			break;
		default:
			break;
		}

	}
	public static void distributTask(ZkTask task) throws Exception{
		distrubtLOG.info("开始分发任务 "+task.convertJson());
		Map<String,String> clientData=new HashMap<>();
		for(String client:task.getClient()){
			String clientPath=ZkTaskPath.getClientTaskPath(task.getType(), client);//ZKPaths.makePath(ZkTaskPath.CLIENT_PATH, client);
			String clietTaskPath=ZKPaths.makePath(clientPath,String.valueOf(task.getTaskid()));
			distrubtLOG.debug("distrubite clietTaskPath="+clietTaskPath);
			if(ZkDataUtils.isExists(clientPath)){
				if(!ZkDataUtils.isExists(clietTaskPath)){
					//					ZkDataUtils.createDataPath(clietTaskPath, task.coventJson());
					clientData.put(clietTaskPath,task.convertJson());
				}else{
					throw new ZkException("clietTaskPath is exists !! "+clietTaskPath);
				}
			}else{
				throw new ZkException("this path is error!!"+clientPath);
			}

		}
		if(!clientData.isEmpty()){
			for(CuratorTransactionResult res:ZkDataUtils.transaction(clientData)){
				System.out.println("事务提交结果："+res.getType());
			}

		}
		if(ZkTaskStatus.valueOf(task.getStat()).equals(ZkTaskStatus.check)){
			
			task.setStat(ZkTaskStatus.create.name());
			ZkDataUtils.setData(task.getZkpath(), task.convertJson());
		}
	}



	public static boolean choiceClient2task(ZkTask zktask,ZkRegistMonitor registClient){
		int num=zktask.getExeNum();
		boolean flag=false;
		if(registClient==null) return flag;
		//如果已经分配不再处理
		if(!zktask.getClient().equals(null) && zktask.getClient().size()>0 && zktask.getClient().size()==num){
			for(String client:zktask.getClient()){
				if(registClient.getRegistCache().containsKey(client)){

					if(registClient.getRegistCache().get(client)<Integer.valueOf(ZKConfig.getClientExeNum())){
						num--;
					}
				}
			}
			if(num==0){
				distrubtLOG.info("user choice "+zktask.getType()+" taskid = "+zktask.getTaskid()+" is exeClient num = "+num);
				registClient.updateRegistCache(zktask.getClient(), 1);
				flag=true;
			}
		}else{
			//不符合实际ip数量，将自动分配
			num=zktask.getExeNum();
			List<String> clients=new ArrayList<>();
			synchronized (registClient) {

				for(Map.Entry<String, Integer> client:registClient.getRegistCache().entrySet()){
					if(client.getValue()<Integer.valueOf(ZKConfig.getClientExeNum())){
						clients.add(client.getKey());
						num--;
					}
					if(num==0){
						zktask.setClient(clients);
						distrubtLOG.info("choice "+zktask.getType()+" taskid = "+zktask.getTaskid()+" is exeClient num = "+num);
						flag=true;
						registClient.updateRegistCache(clients, 1);

						break;
					}else{
						registClient.updateRegistCache(clients, -1);
						distrubtLOG.error("fail choice "+zktask.getType()+" taskid = "+zktask.getTaskid()+" is exeClient num = "+num);
					}
				}


			}
		}
		return flag;
	}

}
