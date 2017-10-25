package com.stp.distribution.framwork;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stp.distribution.util.StringUtils;

public class ZKinstance {
private static Logger log = LoggerFactory.getLogger(ZKinstance.class);
	
	public static CuratorFramework getZKInstance() {
		
		String clienthosts = ZKConfig.getZkClientHosts();
		
		if (StringUtils.isEmpty(clienthosts)) {
			log.error("ERROR! clienthosts is empty!");
			return null;
		}
		
		log.info(ZKConfig.getZkClientHosts()); 
		
		CuratorFramework zkInstance = CuratorFrameworkFactory
			.builder()
			.connectString(ZKConfig.getZkClientHosts())
			.namespace(ZKConfig.getZkNameSpace())
			.sessionTimeoutMs(60000)
			.retryPolicy(new RetryNTimes(20000, 20000))
			.build();
		
		return zkInstance;
	}
	/**
	 * create myNamesapce content,it is only 
	 * @param zkClientHost
	 * @param zkNameSapce
	 * @return
	 */
	public static CuratorFramework getZKInstance(String zkClientHost,String zkNameSapce) {
		
		String clienthosts = ZKConfig.getZkClientHosts();
		
		if (StringUtils.isEmpty(clienthosts)) {
			log.error("ERROR! clienthosts is empty!");
			return null;
		}
		
		log.info(ZKConfig.getZkClientHosts()); 
		
		CuratorFramework zkInstance = CuratorFrameworkFactory
			.builder()
			.connectString(zkClientHost)
			.namespace(zkNameSapce)
			.sessionTimeoutMs(10000)
			.retryPolicy(new RetryNTimes(20000, 10000))
			.build();
		
		return zkInstance;
	}
	

}
