package com.stp.distribution.util;
/**
 * 
 * @author hhbhunter
 *
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CmdExec {
	static Logger log = LoggerFactory.getLogger(CmdExec.class);
	private Message error;
	private Message cmdResult;
	private Process process;



	/**
	 * @param cmd
	 * @param block
	 * @param info
	 * @return status
	 *   IllegalThreadStateException 调用方处理
	 */
	public int cmdExec(String cmd, boolean block, boolean info) {
		return cmdExec(cmd,null,null,info);
	}
	
	public int cmdExec(String[] cmd, String[] envp,File dir, boolean info) {
		if (System.getProperty("os.name").toLowerCase().indexOf("windows") == 0) {
			log.error("IS NOT linux !");
			// throw new java.lang.IllegalStateException("IS NOT linux !");
		}
		int status = -1;
		try {
			process = Runtime.getRuntime().exec(cmd,envp,dir);
			if (info) {
				error = new Message(process.getErrorStream());
				error.start();
				cmdResult = new Message(process.getInputStream());
				cmdResult.start();
			}
			status = process.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
			e.printStackTrace();
		}finally{
			if(process!=null &&!cmdResult.isAlive()&&!error.isAlive() )
			process.destroy();
		}
		return status;
	}
	

	public int cmdExec(String cmd, String[] envp,File dir, boolean info) {
		if (System.getProperty("os.name").toLowerCase().indexOf("windows") == 0) {
			log.info("IS NOT linux. It's WINDOWS ! It will add cmd.exe /c before command, such as cmd.exe /c mvn ... ");
			cmd = "cmd.exe /c " + cmd;
			// throw new java.lang.IllegalStateException("IS NOT linux !");
		}
		int status = -1;
		try {
			System.out.println("cmd is ...... " + cmd);
			process = Runtime.getRuntime().exec(cmd,envp,dir);
			if (info) {
				error = new Message(process.getErrorStream());
				error.start();
				cmdResult = new Message(process.getInputStream());
				cmdResult.start();
			}
			status = process.waitFor();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
			e.printStackTrace();
		}finally{
			if(!cmdResult.isAlive()&&!error.isAlive() )
			process.destroy();
		}
		return status;
	}
	public void stop(){
		if(process!=null)
		process.destroy();
	}

	public int getExitValue(long timeout) {
		long start = System.currentTimeMillis();
		int status = -1;
		while (System.currentTimeMillis() < (start + timeout)) {
			// 异常
			try {
				status = process.exitValue();
			} catch (IllegalThreadStateException e) {
				log.error(e.getMessage());

			}

			if (status > -1)
				break;
		}
//		process.destroy();// 是否销毁
		return status;

	}

	public String getErrorInf() {
		return error.getInfo();
	}

	public String getInfo() {
		return cmdResult.getInfo();
	}
	public String getTmpInfo(){
		return cmdResult.infos.toString();
	}

	class Message extends Thread {
		InputStream in;
		StringBuffer infos = new StringBuffer();

		public Message(InputStream in) {
			this.in = in;
		}

		@Override
		public void run() {
			BufferedReader inRead = new BufferedReader(
					new InputStreamReader(in));
			String inf = "";
			try {
				while ((inf = inRead.readLine()) != null) {
					infos.append(inf).append("\n");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.error(e.getMessage());
				e.printStackTrace();
			} finally {
				try {
					inRead.close();
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					log.error(e.getMessage());
					e.printStackTrace();
				}
			}
		}

		public String getInfo() {
			if (this.isAlive()) {
				log.error("info thread is running !!");
				throw new IllegalThreadStateException(
						"info thread is running !!");
			}
			return infos.toString();
		}
	}
}
