分布式执行：
zkclientconfig.properties 属性zkclient_role=client/manger 来启动不同角色
需实现 TaskResults类中updateDB 更新数据库逻辑
TaskCreaterService 提供add、stop方法，需执行init方法一次(只能一次)


