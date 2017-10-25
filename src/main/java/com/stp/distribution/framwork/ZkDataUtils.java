package com.stp.distribution.framwork;
/**
 * 
 * @author hhbhunter
 *
 */
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stp.distribution.util.StringUtils;


public class ZkDataUtils {
	private static Logger log = LoggerFactory.getLogger(ZkDataUtils.class);
	private static CuratorFramework zkTools;

	static {
		try {
			
			zkTools = ZKinstance.getZKInstance();
			zkTools.getConnectionStateListenable().addListener(new ConnectionStateListener() {

				@Override
				public void stateChanged(CuratorFramework client, ConnectionState newState) {
					log.info("Manager ZKinstance connection state changed to " + newState.toString());
				}
			});
			zkTools.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void setZKinstance(CuratorFramework client){
		zkTools=client;
	}
	public static CuratorFramework getZKinstance(){
		return zkTools;
	}

	public static List<String> getChildren(final String path) throws Exception {
		return zkTools.getChildren().forPath(path);
	}

	public static void setData(final String path, final String data) throws Exception {
		//		log.info("-----Set Data. path:[" + path + "] data:[" + data + "]-----");
		zkTools.setData().forPath(path, data.getBytes(ZKConfig.getZkCharset()));
	}

	public static String  creatPersSequePath(String path) throws Exception{
		return zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path);
	}

	public static String  creatPersPath(String path) throws Exception{
		return zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
	}


	public static void setMapData(final String path, final Map<String, String> dataMap) throws Exception {

		String dataJson = StringUtils.toJson(dataMap);

		setData(path, dataJson);
	}

	public static void setKVData(final String path, final String key, final String value) throws Exception {
		Map<String, String> dataMap = new HashMap<String, String>(1);

		if (isExists(path)) {
			try {
				dataMap = getMapData(path);
			} catch (Exception e) {
				log.error("path="+path);
				log.error("key="+key);
				log.error("value="+value);
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}

		dataMap.put(key, value);

		setMapData(path, dataMap);
	}

	public static Map<String, String> getMapData(final String path) throws Exception {
		String dataJson = getData(path);
		Map<String, String> dataMap;

		if (StringUtils.isEmpty(dataJson)) {
			dataMap = new HashMap<String, String>();
		} else {
			dataMap = StringUtils.getStrMapByJSON(dataJson);
		}

		return dataMap;
	}

	public static String getKVData(final String path, final String key) throws Exception {
		return getMapData(path).get(key);
	}

	public static boolean isExists(final String path) throws Exception {
		boolean checkResult = zkTools.checkExists().forPath(path) != null;
		//		log.info("-----Check Node. path:[" + path + "] state:[" + checkResult + "]-----");
		return checkResult;
	}

	public static String getData(final String path) throws Exception {
		String data = new String(zkTools.getData().forPath(path), ZKConfig.getZkCharset());
//		log.info("-----Get Data. path:[" + path + "] data:[" + data + "]-----");
		return data;
	}

	public static String createDataPathSequential(final String path, final String data) throws Exception {
		//		log.info("-----Create SequentialNode. path:[" + path + "] data:[" + data + "]-----");
		return zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
				.forPath(path, data.getBytes(ZKConfig.getZkCharset()));
	}

	public static void removeDataPath(final String path) throws Exception {
//		log.info("-----Remove Node. path:[" + path + "]-----");
		zkTools.delete().forPath(path);
	}

	public static void createDataPath(final String path, final String data) throws Exception {
		//		log.info("-----Create Node. path:[" + path + "] data:[" + data + "]-----");

		zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data.getBytes(ZKConfig.getZkCharset()));
	}

	public static void createEPHEMERALDataPath(final String path, final String data) throws Exception {
		//		log.info("-----Create Node. path:[" + path + "] data:[" + data + "]-----");
		zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data.getBytes(ZKConfig.getZkCharset()));
	}

	public static void createEPHEMERAL_SEQUENTIALDataPath(final String path, final String data) throws Exception {
		//		log.info("-----Create Node. path:[" + path + "] data:[" + data + "]-----");
		System.out.println("return : "+zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, data.getBytes(ZKConfig.getZkCharset())));
	}

	public static void removeDataPathAndChildren(final String path) throws Exception {
		//		log.info("-----Remove Node and Children. path:[" + path + "]-----");

		removeNodeChildren(path);

		removeDataPath(path);
	}

	public static void removeNodeChildren(final String path) throws Exception {
		log.info("-----Remove Children. path:[" + path + "]-----");
		List<String> pathChildren = zkTools.getChildren().forPath(path);
		if (pathChildren != null && pathChildren.size() > 0) {
			for (String pathChild:pathChildren) {
				removeDataPathAndChildren(ZKPaths.makePath(path, pathChild));
			}
		}
	}

	public static  Collection<CuratorTransactionResult> transaction(Map<String,String> data) throws Exception{
		CuratorTransactionBridge transaction = null;
		boolean flag=true;
		for(Map.Entry<String,String> my: data.entrySet()){
			if(flag){
				transaction=zkTools.inTransaction().create().forPath(my.getKey(),my.getValue().getBytes(ZKConfig.getZkCharset()));
				flag=false;
			}else{
				transaction.and().create().forPath(my.getKey(),my.getValue().getBytes(ZKConfig.getZkCharset()));
			}
		}
		return transaction.and().commit();

	}
}
