# redis_task_consume
		redis的消费端，可以把list中的脚本命令取出后执行。可以通过调整参数设置最大的后台任务数，和心跳超时时间。		
		脚本命令需要向管道文件 “日志目录/script_{进程Id}” 不断的写入心跳时间。		
		脚本命令行最后一个参数作为redis hset中的key，父进程会更新脚本的信息到此key		
		需要hiredis的头文件和动态库。		
		启动参数参考: task_monitor -h172.16.0.193 -p6379 -l/data/www/otm_online/otm/runtime		
		-h:redis服务器地址		
		-p:redis服务端口		
		-l:日志文件和心跳管道文件目录		
		-t:子任务超时时间		
		-n:最大子进程数目		
