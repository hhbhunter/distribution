package com.stp.distribution.client;
/**
 * @author hhbhunter
 */
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.concurrent.Callable;

import com.stp.distribution.entity.LogEntity;
import com.stp.distribution.framwork.ZkDataUtils;
import com.stp.distribution.util.CmdExec;

public class ZkClientLog implements Callable<LogEntity>{
	private static FileReader in = null;
	private static LineNumberReader reader = null;
	LogEntity logData=new LogEntity();
	CmdExec currExec;
	int lineNum=0;
	String myTaskLogPaht="";
	public ZkClientLog(LogEntity logData,CmdExec currExec,String myTaskLogPaht){
		this.logData=logData;
		this.currExec=currExec;
		this.myTaskLogPaht=myTaskLogPaht;
		try {
			ZkDataUtils.createDataPath(myTaskLogPaht, "start...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public ZkClientLog(){

	}
	public LogEntity getLogData() {
		return logData;
	}
	public void setLogData(LogEntity logData) {
		this.logData = logData;
	}
	@Override
	public LogEntity call(){
		while(!logData.isFinish()){

			try{
				Thread.sleep(1500);
			}catch(Exception e){

			}
			getLog(logData);
			updateLog();
		}

		closeStream();
		if(logData.isAuto()){
			//jmeterbug 引入 自动停止标识
			autoStop(logData.getTaskId());
		}
		return logData;
	}
	/**
	 * jmeter bug 引入
	 * @param taskid
	 */
	public  void autoStop(String taskid){
		String cmd="ps -ef | grep -v grep | grep "+taskid+" | awk '{print $2}' | xargs kill ";
		int stat=-1;
		if(cmd.contains("|")){
			stat=currExec.cmdExec(new String[]{"sh","-c",cmd},null,null,true);
		}else{
			stat=currExec.cmdExec(cmd,null,null,true);
		}
		if(stat==0){
			System.out.println("autoStop taskid="+taskid+" success!!");
		}else{
			System.out.println("autoStop taskid="+taskid+" failed!!");
		}
		
	}
	public static LogEntity getLog(LogEntity log) {

		String content = "";
		int lineNum = log.getLineNum();
		try {
			in = new FileReader(new File(log.getPath()));
			reader = new LineNumberReader(in);
			reader.setLineNumber(log.getLineNum());
			String strLine = null;
			while ((strLine= reader.readLine()) != null) {
				content+= strLine + System.getProperty("line.separator");;
				lineNum++;
				if(strLine.contains("... end of run")){
					if(!log.isFinish()){
						
						log.setFinish(true);
						log.setAuto(true);
					}
				}
			}
			log.setContent(content);
			log.setLineNum(lineNum);
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return log;
	}

	void closeStream(){

		try {
			if(reader != null){
				reader.close();
				if(in !=null){
					in.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	void updateLog(){
		if(lineNum<logData.getLineNum()){
			try {
				if(ZkDataUtils.isExists(myTaskLogPaht)){
					ZkDataUtils.setData(myTaskLogPaht, logData.getContent());
				}else{
					logData.setFinish(true);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		lineNum=logData.getLineNum();


	}

}
