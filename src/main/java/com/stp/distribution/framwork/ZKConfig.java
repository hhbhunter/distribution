package com.stp.distribution.framwork;
/**
 * @author hhbhunter
 */
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKConfig {
	private static Properties props = new Properties();
	private static Logger log = LoggerFactory.getLogger("zkclient");
	static {
		ZKConfig.readConfig("config/zkclientconfig.properties");
	}
	public static boolean readConfig(String configPath) {
		InputStream fileinputstream = null;
		try {
			fileinputstream = new FileInputStream(configPath);
		} catch (FileNotFoundException e1) { 
			log.info("readConfig : " + configPath + "doesn't exist in local path");
		}
		
		if(fileinputstream==null){
			fileinputstream = ZKConfig.class.getResourceAsStream("/"+configPath);
		}
		if (fileinputstream == null) {
			log.error(configPath + " is not in the classpath. Please check it.");
			return false;
		}
		try {
			props.load(fileinputstream);
		} catch (IOException e) {
			log.error(configPath+" resolve fail!");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public static String getRole(){
		return props.getProperty("zkclient_role","client");
	}
	public static String getZkClientHosts(){
		return props.getProperty("zkclient_clienthosts");
	}
	public static String getZkNameSpace() {
		return props.getProperty("zkclient_namespace");
	}
	public static String getZkCharset() {
		return props.getProperty("zkclient_charset");
	}
	public static String getClientExeNum(){
		return props.getProperty("zkclient_exenum","4");
	}

}
