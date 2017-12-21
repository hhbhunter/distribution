#############################
#	分布式任务执行
# 
#############################
一、本项目是基于zookeeper分布式执行调度：
	1、支持同一个client并行执行多个任务
	2、支持任务随机选择执行client和指定client两种方式
	3、可以根据任务量大小设置多个client同时执行，详细见com.stp.distribution.entity.ZkTask

二、分为3个角色：client、user、manager
1、client	负责命令执行
2、user	          创建有序任务
3、manager	管控中心，负责任务的管理分发

三、使用说明：
1、zkclientconfig.properties 
	属性zkclient_role=client/manger 来启动不同角色，zkclient_exenum=1指定单个client同时运行任务数
2、需实现 TaskResults类中接口：
	updateDB 回调更新业务状态逻辑，以便持久化
	scpFile 文件处理
3、TaskCreaterService 提供add/stop/getLog/getClients方法，供外部业务接入




