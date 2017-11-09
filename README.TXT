
本项目是基于zookeeper分布式执行调度：支持同一个client并行执行多个任务

设计分为3个角色：client、user、manager
2、client 	负责命令执行
3、user 		创建有序任务
4、manager 	管控中心，负责任务的管理分发

使用说明：
1、zkclientconfig.properties 
	属性zkclient_role=client/manger 来启动不同角色，zkclient_exenum=1指定单个client同时运行任务数
2、需实现 TaskResults类中updateDB 更新数据库逻辑
3、TaskCreaterService 提供add、stop方法，供外部业务接入




