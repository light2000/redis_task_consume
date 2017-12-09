/*
 ============================================================================
 Name        : task_monitor.c
 Author      : light2000
 Version     : 1.0
 Description : Asynchronous Task Queue Consumer, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <net/if.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>

#include "hiredis.h"
#include "otm_task_monitor.h"
/**
 redis 配置
 **/
static char redisHost[15] = { 0 };
static int redisPort = 0;
//日志文件和心跳文件目录
static char logPath[100] = { 0 };
//退出状态
static int quit_status = 0;

int main(int argc, char *argv[]) {

	char cmdTmp[MAX_CMD_LENTH]; //获取队列中的执行脚本
	char cmdTmp2[MAX_CMD_LENTH]; //用来移除TASK_WAITTING_HSET中的脚本KEY
	char cmdKeys[MAX_CHILD_PROCESS_NUMS][MAX_CHAR_EACH_PARA]; //redis中对应任务的键名
	char cmdContent[MAX_CHILD_PROCESS_NUMS][MAX_CMD_LENTH]; //执行脚本的集合

	int i = 0;
	int childProcess[MAX_CHILD_PROCESS_NUMS] = { 0 }; //脚本的进程号集合

	time_t processLiveTime[MAX_CHILD_PROCESS_NUMS] = { 0 }; //脚本的心跳时间
	time_t processLaunchTime[MAX_CHILD_PROCESS_NUMS] = { 0 }; //脚本的启动时间

	int status; //for waitpid
	pid_t ret; //waitpid result

	char buffer[800];

	time_t tt;
	time_t liveTime;

	struct tm *lt; //心跳时间

	int killResult;

	int redisRet = 0;

	//int macRet;
	//char mac[32];
	//char machine[50];

	int maxChildNum = 10;	//最大脚本(子进程)数量
	int childTimeOut = 30;	//脚本超时时间(秒)
	char maxChildNumFromRedis[5];	//最大脚本(子进程)数量(redis获取)
	char childTimeOutFromRedis[10];	//脚本超时时间(秒) (redis获取)
	char stopStatus[10];	//脚本终止信号(redis获取)

	int tmp;

	int scriptflush = 0;

	//char otmMessage[30];

	int running = 0;	//是否退出monitor

	int oc;

	if (NULL != argv[1] && 0 == strcasecmp(argv[1], "stop")) {
		if (NULL != argv[2] && 0 == strcasecmp(argv[2], "--force")) {
			sprintf(buffer, "killall %s", PROJECT_NAME);//停止从队列获取新的任务脚本,强制终止目前在运行的任务脚本，并退出。
		} else {
			sprintf(buffer, "killall -2 %s", PROJECT_NAME);	//停止从队列获取新的任务脚本，但是不强制终止目前执行的子进程(任务脚本),等待进行中的任务结束后终止monitor
		}
		shellExc(buffer);
		exit(0);
	}

	while ((oc = getopt(argc, argv, "h:p:n:t:l:")) != -1) {
		switch (oc) {
		case 'h':
			strncpy(redisHost, optarg, sizeof(redisHost));
			break;
		case 'p':
			redisPort = atoi(optarg);
			break;
		case 'n':
			maxChildNum = atoi(optarg);
			break;
		case 't':
			childTimeOut = atoi(optarg);
			break;
		case 'l':
			strncpy(logPath, optarg, sizeof(logPath));
			break;
		case '?':
			break;
		case ':':
			break;
		}
	}

	if (1 == OTM_DEBUG) {
		strcpy(redisHost, "172.16.0.193");
		redisPort = 6379;
		strcpy(logPath, "/data/www/otm_online/otm/runtime");
	}

	if (0 == strlen(logPath)) {
		printf("log path is not set, use -l set it\n");
		exit(0);
	}

	int logTest = writeLog(LOG_NOTICE,
			"==============script start============");
	if (OTM_ERR == logTest) {
		printf("log path is not writable\n");
		exit(0);
	}

	if (0 == strlen(redisHost)) {
		printf("redis host is not set, use -h set it");
		exit(0);
	}

	if (0 == redisPort) {
		printf("redis port is not set, use -p set it");
		exit(0);
	}
//最大子进程数，不超过100
	if (maxChildNum > MAX_CHILD_PROCESS_NUMS) {
		maxChildNum = MAX_CHILD_PROCESS_NUMS;
	}
//重写ctrl+c和kill信号相应程序
	initOtm();

	writeLog(LOG_NOTICE,
			"redis host %s, port %d, max child number %d, child timeout seconds %d",
			redisHost, redisPort, maxChildNum, childTimeOut);

	time(&tt);

	//shellExc("/usr/local/php-5.4.33/bin/php /htdocs/shell.php 100");

	while (1) {
		running = 1;

		memset(maxChildNumFromRedis, 0, sizeof(maxChildNumFromRedis));
		hgetRedis(TASK_SETTING_HSET, "max_process_num", maxChildNumFromRedis,
				sizeof(maxChildNumFromRedis), REDIS_RETRY_TIMES);
		if (strlen(maxChildNumFromRedis) > 0) {
			tmp = atoi(maxChildNumFromRedis);
			if (tmp > 0 && maxChildNum != tmp && tmp < MAX_CHILD_PROCESS_NUMS) {
				maxChildNum = tmp;
				writeLog(LOG_WARNING, "max process number is change to %d",
						maxChildNum);
			}
		}

		memset(childTimeOutFromRedis, 0, sizeof(childTimeOutFromRedis));
		hgetRedis(TASK_SETTING_HSET, "process_heartbeat_time",
				childTimeOutFromRedis, sizeof(childTimeOutFromRedis),
				REDIS_RETRY_TIMES);
		if (strlen(childTimeOutFromRedis) > 0) {
			tmp = atoi(childTimeOutFromRedis);
			if (tmp > 0 && childTimeOut != tmp) {
				childTimeOut = tmp;
				writeLog(LOG_WARNING,
						"heart beat check time is change to %d seconds",
						childTimeOut);
			}
		}

		for (i = 0; i < MAX_CHILD_PROCESS_NUMS; i++) {
			if (0 == childProcess[i] && quit_status == 0 && i < maxChildNum) {
				memset(cmdTmp, 0, MAX_CMD_LENTH);
				memset(cmdTmp2, 0, MAX_CMD_LENTH);
				memset(cmdKeys[i], 0, MAX_CHAR_EACH_PARA);
				memset(cmdContent[i], 0, MAX_CMD_LENTH);
				//从队列获取任务脚本
				popRedis(TASK_QUEUE_NAME, cmdTmp, MAX_CMD_LENTH - 1);
				time(&tt);
				if (strlen(cmdTmp) > 1) {
					//执行任务脚本
					childProcess[i] = forkRun(cmdTmp, cmdKeys[i]);
					if (childProcess[i] > 0) {
						writeLog(LOG_NOTICE,
								"begin run script: %s, process id %d", cmdTmp,
								childProcess[i]);
						strncpy(cmdContent[i], cmdTmp, strlen(cmdTmp));

						strncpy(cmdTmp2, cmdTmp,
								strlen(cmdTmp) - strlen(cmdKeys[i]) - 1);

						hdelRedis(TASK_WAITTING_HSET, cmdTmp2,
						REDIS_RETRY_TIMES);

						redisRet = initTask(childProcess[i], cmdTmp,
								cmdKeys[i]);
						time(&processLiveTime[i]);
						time(&processLaunchTime[i]);
						if (OTM_OK != redisRet) {
							writeLog(LOG_ERROR,
									"script: %s build failed, process id %d",
									cmdTmp, childProcess[i]);
						}
					} else {
						redisRet = pushRedis(TASK_QUEUE_NAME, cmdTmp,
						REDIS_RETRY_TIMES);
						if (OTM_OK == redisRet) {
							writeLog(LOG_NOTICE,
									"script: %s fork failed, add to task queue again",
									cmdTmp);
						}
					}
				} else {
					writeLog(LOG_DEBUG, "shell script not found in queue: %d",
							i);
				}

				usleep(100000);
			} else if (0 != childProcess[i]) {
				scriptflush = 0;
				time(&tt);
				sprintf(buffer, "%d", (int) tt - (int) processLaunchTime[i]);
				hsetRedis(cmdKeys[i], "runtime", buffer, 0,
				REDIS_RETRY_TIMES);
				//检查任务脚本是否已经退出
				ret = waitpid(childProcess[i], &status, WNOHANG);
				if (ret == childProcess[i]) {
					sprintf(buffer, "%d", (int) tt);
					hsetRedis(cmdKeys[i], "terminate_time", buffer, 0,
					REDIS_RETRY_TIMES);
					saddRedis(TASK_FINISHED_SET, cmdKeys[i], REDIS_RETRY_TIMES);
					writeLog(LOG_NOTICE, "script finished: %s %d",
							cmdContent[i], ret);
					scriptflush = 1;
				} else if (-1 == ret) {
					writeLog(LOG_ERROR,
							"child process recovery failed, script %s,task id %s %s",
							cmdContent[i], cmdKeys[i], strerror(errno));
					saddRedis(TASK_FINISHED_SET, cmdKeys[i], REDIS_RETRY_TIMES);
					scriptflush = 1;
				} else {
					//检查心跳和中止信号(stopped)
					memset(stopStatus, 0, sizeof(stopStatus));
					hgetRedis(cmdKeys[i], "stopped", stopStatus,
							sizeof(stopStatus), REDIS_RETRY_TIMES);

					liveTime = (time_t) getTaskLivetime(childProcess[i]);
					if (liveTime > 0) {
						processLiveTime[i] = liveTime;
					}

					lt = localtime(&processLiveTime[i]);
					//script timeout
					if (tt - processLiveTime[i] > childTimeOut
							|| STOP_FORCE == quit_status
							|| strcasecmp(stopStatus, "1") == 0) {
						killResult = killProcess(childProcess[i]);
						sprintf(buffer, "%d", (int) tt);
						hsetRedis(cmdKeys[i], "interrupt_time", buffer, 0,
						REDIS_RETRY_TIMES);
						sprintf(buffer, "%d", killResult);
						hsetRedis(cmdKeys[i], "interrupt_status", buffer, 0,
						REDIS_RETRY_TIMES);

						sprintf(buffer, "process %s:%d is timeout",
								cmdContent[i], childProcess[i]);
						if (STOP_FORCE == quit_status) {
							hsetRedis(cmdKeys[i], "interrupt_info",
									"monitor is drop out", 0,
									REDIS_RETRY_TIMES);
						} else if (strcasecmp(stopStatus, "1") == 0) {
							hsetRedis(cmdKeys[i], "interrupt_info",
									"stopped by user", 0,
									REDIS_RETRY_TIMES);
						} else {
							hsetRedis(cmdKeys[i], "interrupt_info", buffer, 0,
							REDIS_RETRY_TIMES);
						}
						saddRedis(TASK_KILLED_SET, cmdKeys[i],
						REDIS_RETRY_TIMES);

						scriptflush = 1;

						writeLog(LOG_ERROR,
								"process %s:%d is killed, quit status %d last live time %4d-%02d-%02d %02d:%02d:%02d",
								cmdContent[i], childProcess[i], quit_status,
								lt->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,
								lt->tm_hour, lt->tm_min, lt->tm_sec);
					} else {
						writeLog(LOG_DEBUG,
								"process %s:%d is running, last live time %4d-%02d-%02d %02d:%02d:%02d",
								cmdContent[i], childProcess[i],
								lt->tm_year + 1900, lt->tm_mon + 1, lt->tm_mday,
								lt->tm_hour, lt->tm_min, lt->tm_sec);
					}
				}
				if (1 == scriptflush) {
					sremRedis(TASK_RUN_SET, cmdKeys[i], REDIS_RETRY_TIMES);
					sprintf(buffer, "%s/script_%d", logPath, childProcess[i]);
					if (-1 != access(buffer, F_OK)) {
						unlink(buffer);
					} else {
						writeLog(LOG_ERROR,
								"script %s heart beat file %s remove failed %s",
								cmdContent[i], buffer, strerror(errno));
					}
					childProcess[i] = 0;
					memset(cmdKeys[i], 0, MAX_CHAR_EACH_PARA);
					memset(cmdContent[i], 0, MAX_CMD_LENTH);
				}
				usleep(100000);
			}

		}
		if (0 != quit_status) {
			running = 0;
			for (i = 0; i < MAX_CHILD_PROCESS_NUMS; i++) {
				if (0 != childProcess[i]) {
					running = 1;
				}
			}
			if (0 == running) {
				break;
			}
		}
	}

	writeLog(LOG_NOTICE, "=========script end=========");

	buildRedisContext(-1);

	return 0;
}

void initOtm() {
	struct sigaction new, old;
	char buffer[300];
	int ret;
	new.sa_handler = sig_handler;
	sigemptyset(&new.sa_mask);
	new.sa_flags = 0;
	sigaction(SIGTERM, &new, &old);
	sigaction(SIGINT, &new, &old);

	//test heart beat fifo writable
	sprintf(buffer, "%s/heart_beat_test", logPath);

	ret = access(buffer, F_OK);
	if (-1 != ret) {
		do {
			ret = unlink(buffer);
		} while (-1 == ret);
	}
	ret = mkfifo(buffer, 0777);
	if (ret != 0) {
		writeLog(LOG_ERROR, "unable create script heart beat file: err %s",
				strerror(errno));
		printf("unable create script heart beat file: %s", strerror(errno));
		exit(0);
	} else {
		unlink(buffer);
	}
}

int initTask(int processId, const char *script, const char *taskId) {
	char buffer[120];
	int ret;

	time_t tt;
	time(&tt);

	sprintf(buffer, "%s/script_%d", logPath, processId);

	ret = access(buffer, F_OK);
	if (-1 != ret) {
		do {
			ret = unlink(buffer);
		} while (-1 == ret);
	}
	ret = mkfifo(buffer, 0777);
	if (ret != 0) {
		writeLog(LOG_ERROR,
				"create script heart beat file failed: task id %s, process id %d, command %s, err %s",
				taskId, processId, script, strerror(errno));
	}

	sprintf(buffer, "%d", (int) tt);
	hsetRedis(taskId, "launch_time", buffer, 0,
	REDIS_RETRY_TIMES);

	hsetRedis(taskId, "live_time", buffer, 0,
	REDIS_RETRY_TIMES);

	hsetRedis(taskId, "runtime", "0", 0,
	REDIS_RETRY_TIMES);

	sprintf(buffer, "%d", processId);
	hsetRedis(taskId, "process_id", buffer, 0,
	REDIS_RETRY_TIMES);

	hsetRedis(taskId, "status", STATUS_LAUNCHED, 1,
	REDIS_RETRY_TIMES);

	return saddRedis(TASK_RUN_SET, taskId, REDIS_RETRY_TIMES);
}

int getTaskLivetime(int processId) {
	char heartBeatFile[120];
	char buffer[PIPE_BUF + 1];
	char livetime[10];

	int ret;
	int pipe_fd = -1;
	//int size;

	sprintf(heartBeatFile, "%s/script_%d", logPath, processId);

	memset(livetime, '\0', sizeof(livetime));
	memset(buffer, '\0', sizeof(buffer));

	pipe_fd = open(heartBeatFile, O_RDONLY | O_NONBLOCK);

	if (pipe_fd != -1) {
		do {
			ret = read(pipe_fd, buffer, PIPE_BUF);
		} while (0);
		close(pipe_fd);
	}

	strncpy(livetime, buffer, 10);

	if (strlen(livetime) < 1) {
		return 0;
	}

	ret = atoi(livetime);

	writeLog(LOG_DEBUG, "process %d live time %d", processId, ret);

	return ret;
}

int hsetRedis(const char *key, const char *field, const char *value, int nx,
		int retryTimes) {
	redisContext* redisConn;
	redisReply *reply;

	redisConn = buildRedisContext(0);

	if (1 == nx) {
		reply = (redisReply *) redisCommand(redisConn, "HSETNX %s %s %s", key,
				field, value);
	} else {
		reply = (redisReply *) redisCommand(redisConn, "HSET %s %s %s", key,
				field, value);
	}

	if (NULL == reply) {
		writeLog(LOG_ERROR, "HSET FAILED: %s %s %s, err info %s", key, field,
				value, redisConn->errstr);
		return OTM_ERR;
	}

	if (REDIS_REPLY_INTEGER == reply->type) {
		writeLog(LOG_DEBUG, "HSET SUCCESS, command HSET %s %s %s! result %d",
				key, field, value, reply->integer);
	} else {
		freeReplyObject(reply);
		reply = NULL;
		writeLog(LOG_ERROR, "HSET FAILED, command HSET %s %s %s, retry %d", key,
				field, value, retryTimes);
		if (retryTimes > 0) {
			retryTimes--;
			return hsetRedis(key, field, value, nx, retryTimes);
		}

		return OTM_ERR;
	}

	freeReplyObject(reply);
	reply = NULL;

	return OTM_OK;
}

int hgetRedis(const char *key, const char *field, char *result, int resultLen,
		int retryTimes) {
	redisContext* redisConn;
	redisReply *reply;

	redisConn = buildRedisContext(0);
	reply = (redisReply *) redisCommand(redisConn, "HGET %s %s", key, field);

	if (NULL == reply) {
		writeLog(LOG_ERROR, "HGET FAILED, command HGET %s %s, err info %s", key,
				field, redisConn->errstr);
		return OTM_ERR;
	}

	if (REDIS_REPLY_STRING == reply->type) {
		writeLog(LOG_DEBUG, "HGET SUCCESS, command HGET %s %s, result %s!", key,
				field, reply->str);
		if (NULL != reply->str && strlen(reply->str) > 0) {
			resultLen =
					resultLen > strlen(reply->str) ?
							strlen(reply->str) : resultLen;
			strncpy(result, reply->str, resultLen);
		}
	} else if (REDIS_REPLY_NIL == reply->type) {
		memset(result, 0, resultLen);
		writeLog(LOG_DEBUG, "HGET NIL, command HGET %s %s!", key, field);
	} else {
		freeReplyObject(reply);
		reply = NULL;

		writeLog(LOG_ERROR, "HGET FAILED, command HGET %s %s, retry %d", key,
				field, retryTimes);

		if (retryTimes > 0) {
			retryTimes--;
			return hgetRedis(key, field, result, resultLen, retryTimes);
		}

		return OTM_ERR;
	}

	freeReplyObject(reply);
	reply = NULL;

	return OTM_OK;
}

int hdelRedis(const char *key, const char *field, int retryTimes) {
	redisContext* redisConn;
	redisReply *reply;

	redisConn = buildRedisContext(0);

	reply = (redisReply *) redisCommand(redisConn, "HDEL %s %s", key, field);

	if (NULL == reply) {
		writeLog(LOG_ERROR, "HDEL FAILED: %s %s, err info %s", key, field,
				redisConn->errstr);
		return OTM_ERR;
	}

	if (REDIS_REPLY_INTEGER == reply->type) {
		writeLog(LOG_DEBUG, "HDEL SUCCESS, command HDEL %s %s! result %d", key,
				field, reply->integer);
	} else {
		freeReplyObject(reply);
		reply = NULL;
		writeLog(LOG_ERROR, "HDEL FAILED, command HDEL %s %s, retry %d", key,
				field, retryTimes);
		if (retryTimes > 0) {
			retryTimes--;
			return hdelRedis(key, field, retryTimes);
		}

		return OTM_ERR;
	}

	freeReplyObject(reply);
	reply = NULL;

	return OTM_OK;
}

int popRedis(const char *queueName, char *result, int resultSize) {
	redisContext* redisConn;
	redisReply *reply;

	redisConn = buildRedisContext(0);
	reply = (redisReply *) redisCommand(redisConn, "RPOP %s", queueName);

	if (NULL == reply) {
		writeLog(LOG_ERROR, "RPOP FAILED, command RPOP %s, err info %s",
				queueName, redisConn->errstr);
		return OTM_ERR;
	}

	if (REDIS_REPLY_STRING == reply->type) {
		if (NULL != reply->str && strlen(reply->str) > 0) {
			strncpy(result, reply->str, resultSize);
			writeLog(LOG_DEBUG, "RPOP SUCCESS ,command RPOP %s", queueName);
		} else {
			writeLog(LOG_ERROR, "EMPTY COMMAND FOUND ,command RPOP %s",
					queueName);
		}
	} else if (REDIS_REPLY_NIL == reply->type) {
		writeLog(LOG_DEBUG, "QUEUE EMPTY, command RPOP %s", queueName);
	} else {
		writeLog(LOG_ERROR, "RPOP FAILED, command RPOP %s", queueName);
	}

	freeReplyObject(reply);
	reply = NULL;

	return OTM_OK;
}

int pushRedis(const char *queueName, const char *value, int retryTimes) {
	redisContext* redisConn;
	redisReply *reply;

	redisConn = buildRedisContext(0);
	reply = (redisReply *) redisCommand(redisConn, "LPUSH %s %s", queueName,
			value);

	if (NULL == reply) {
		writeLog(LOG_ERROR, "LPUSH FAILED, command LPUSH %s %s, err info %s",
				queueName, value, redisConn->errstr);
		return OTM_ERR;
	}

	if (REDIS_REPLY_INTEGER == reply->type) {
		writeLog(LOG_DEBUG,
				"LPUSH SUCCESS ,command LPUSH %s %s, list length %d!",
				queueName, value, reply->integer);
	} else {
		freeReplyObject(reply);
		reply = NULL;
		writeLog(LOG_ERROR, "LPUSH FAILED, command LPUSH %s %s", queueName,
				value);

		if (retryTimes > 0) {
			retryTimes--;
			return pushRedis(queueName, value, retryTimes);
		}

		return OTM_ERR;
	}

	freeReplyObject(reply);
	reply = NULL;

	return OTM_OK;
}

int saddRedis(const char *setName, const char *value, int retryTimes) {
	redisContext* redisConn;
	redisReply *reply;

	redisConn = buildRedisContext(0);
	reply = (redisReply *) redisCommand(redisConn, "SADD %s %s", setName,
			value);

	if (NULL == reply) {
		writeLog(LOG_ERROR, "SADD FAILED, command SADD %s %s, err info %s",
				setName, value, redisConn->errstr);
		return OTM_ERR;
	}

	if (REDIS_REPLY_INTEGER == reply->type) {
		writeLog(LOG_DEBUG, "SADD SUCCESS ,command SADD %s %s!", setName,
				value);
	} else {
		freeReplyObject(reply);
		reply = NULL;

		writeLog(LOG_ERROR, "SADD FAILED ,command SADD %s %s, retry %d",
				setName, value, retryTimes);

		if (retryTimes > 0) {
			retryTimes--;
			return saddRedis(setName, value, retryTimes);
		}

		return OTM_ERR;
	}

	freeReplyObject(reply);
	reply = NULL;

	return OTM_OK;
}

int sremRedis(const char *setName, const char *value, int retryTimes) {
	redisContext* redisConn;
	redisReply *reply;

	redisConn = buildRedisContext(0);
	reply = (redisReply *) redisCommand(redisConn, "SREM %s %s", setName,
			value);

	if (NULL == reply) {
		writeLog(LOG_ERROR, "SREM FAILED, command SREM %s %s, err info %s",
				setName, value, redisConn->errstr);
		return OTM_ERR;
	}

	if (REDIS_REPLY_INTEGER == reply->type) {
		writeLog(LOG_DEBUG, "SREM SUCCESS, command SREM %s %s!", setName,
				value);
	} else {
		freeReplyObject(reply);
		reply = NULL;

		writeLog(LOG_ERROR, "SREM FAILED, command SREM %s %s, retry %d",
				setName, value, retryTimes);

		if (retryTimes > 0) {
			retryTimes--;
			return sremRedis(setName, value, retryTimes);
		}

		return OTM_ERR;
	}

	freeReplyObject(reply);
	reply = NULL;

	return OTM_OK;
}

redisContext* buildRedisContext(int sleepSecond) {
	static redisContext *redisConn = NULL;
	static int retryTimes = 0;
	static time_t lastConnectTime;
	static struct timeval timeout = { 1, 500000 };

	redisReply *reply;
	time_t tt;

	if (sleepSecond > 0) {
		redisFree(redisConn);
		redisConn = NULL;
		retryTimes++;
		writeLog(LOG_NOTICE,
				"try to reconnect redis server %s:%d,wait second: %d, retry times %d",
				redisHost, redisPort, sleepSecond, retryTimes);
		sleep(sleepSecond);
		return buildRedisContext(0);
	} else if (-1 == sleepSecond) {
		redisFree(redisConn);
		redisConn = NULL;
		return redisConn;
	} else if (0 == sleepSecond) {
		if (redisConn == NULL) {
			redisConn = redisConnectWithTimeout(redisHost, redisPort, timeout);
			if (redisConn == NULL || redisConn->err) {
				if (redisConn) {
					writeLog(LOG_ERROR, "connect redis error: %s",
							redisConn->errstr);
				} else {
					writeLog(LOG_ERROR, "connect redis server failed");
				}
				if (0 != quit_status) {
					exit(0);
				}
				return buildRedisContext(3);
			} else {
				writeLog(LOG_NOTICE, "redis server %s:%d connected!",
						redisConn->tcp.host, redisConn->tcp.port);
				time(&lastConnectTime);
			}
		}

		time(&tt);
		if (tt - lastConnectTime <= 3) {
			return redisConn;
		}
//check connection every 3 second
		reply = (redisReply*) redisCommand(redisConn, "PING");
		if (reply == NULL) {
			writeLog(LOG_DEBUG, "PING Failed, last connected %d second!",
					tt - lastConnectTime);
			return buildRedisContext(3);
		}

		if (reply->type != REDIS_REPLY_STATUS
				|| strcasecmp(reply->str, "PONG") != 0) {
			writeLog(LOG_DEBUG,
					"PING Failed, response type %d, integer %d, string %s!",
					reply->type, reply->integer, reply->str);
			freeReplyObject(reply);
			return buildRedisContext(3);
		}

		freeReplyObject(reply);
	}

	retryTimes = 0; //reset retry times

	return redisConn;
}

int forkRun(char *cmd, char *taskKey) {
	int pid;
	int counter = 0;
	char params[MAX_PARA_NUMS][MAX_CHAR_EACH_PARA];

	counter = split(cmd, params);

	pid = fork();
	if (pid == 0) {
		switch (counter) {
		case 0:
			break;
		case 1:
			execlp(params[0], params[0], (char*) 0);
			break;
		case 2:
			execlp(params[0], params[0], params[1], (char*) 0);
			break;
		case 3:
			execlp(params[0], params[0], params[1], params[2], (char*) 0);
			break;
		case 4:
			execlp(params[0], params[0], params[1], params[2], params[3],
					(char*) 0);
			break;
		case 5:
			execlp(params[0], params[0], params[1], params[2], params[3],
					params[4], (char*) 0);
			break;
		case 6:
			execlp(params[0], params[0], params[1], params[2], params[3],
					params[4], params[5], (char*) 0);
			break;
		case 7:
			execlp(params[0], params[0], params[1], params[2], params[3],
					params[4], params[5], params[6], (char*) 0);
			break;
		case 8:
			execlp(params[0], params[0], params[1], params[2], params[3],
					params[4], params[5], params[6], params[7], (char*) 0);
			break;
		case 9:
			execlp(params[0], params[0], params[1], params[2], params[3],
					params[4], params[5], params[6], params[7], params[8],
					(char*) 0);
			break;
		case 10:
			execlp(params[0], params[0], params[1], params[2], params[3],
					params[4], params[5], params[6], params[7], params[8],
					params[9], (char*) 0);
			break;
		default:
			writeLog(LOG_ERROR,
					"command parameters number is overload: %s | %d", cmd,
					counter);
			break;
		}

//hsetRedis(cmd, "status", STATUS_ERR, 0);
//hsetRedis(cmd, "error_msg", strerror(errno), 0);
		writeLog(LOG_ERROR, "command execute failed: %s | %s", strerror(errno),
				cmd);

		exit(0);
	} else if (pid < 0) {
		writeLog(LOG_ERROR, "fork failed: %s | %s", strerror(errno), cmd);
	} else {
		strncpy(taskKey, params[counter - 1], MAX_CHAR_EACH_PARA);
	}

	return pid;
}

int split(char *input, char output[MAX_PARA_NUMS][MAX_CHAR_EACH_PARA]) {
	int counter = 0;
	int len = strlen(input);
	int bTemp = 0;
	int i, j = 0;

	for (i = 0; i < len; i++) {
		if (' ' == input[i]) {
			if (j != 0 && counter > 0) {
				output[counter - 1][j] = '\0';
			}
			bTemp = 0;
		} else {
			if (bTemp == 0) {
				if (counter >= MAX_PARA_NUMS) {
					break;
				}
				j = 0;
				counter++;
				bTemp = 1;
			}
			if (j >= MAX_CHAR_EACH_PARA - 1) {
				output[counter - 1][j] = '\0';
			} else {
				output[counter - 1][j] = input[i];
				j++;
			}
		}
	}

	output[counter - 1][j] = '\0';
	return counter;
}

int shellExc(const char *command) {
	char buf[128];
	FILE *pp;

	if ((pp = popen(command, "r")) == NULL) {
		printf("popen() error!/n");
		exit(1);
	}

	while (fgets(buf, sizeof buf, pp)) {
		printf("%s", buf);
	}
	pclose(pp);

	return 0;
}

int writeLog(int logLevel, const char *logFormat, ...) {

	va_list ap;

	struct tm *t;
	time_t tt;

	char logContent[1030];
	char buffer[1000];
	char logFile[100];

	int ret;

	if (logLevel == LOG_DEBUG && 0 == OTM_DEBUG) {
		return OTM_OK;
	}

	va_start(ap, logFormat);
	ret = vsprintf(buffer, logFormat, ap);
	va_end(ap);

	if (ret < 0) {
		return OTM_ERR;
	}

	time(&tt);
	t = localtime(&tt);

	sprintf(logContent, "%4d-%02d-%02d %02d:%02d:%02d \"%s\"\n",
			t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour, t->tm_min,
			t->tm_sec, buffer);
	if (LOG_ERROR == logLevel) {
		sprintf(logFile, "%s/%4d-%02d-%02d.error.log", logPath,
				t->tm_year + 1900, t->tm_mon + 1, t->tm_mday);
	} else {
		sprintf(logFile, "%s/%4d-%02d-%02d.log", logPath, t->tm_year + 1900,
				t->tm_mon + 1, t->tm_mday);
	}

	//printf(logContent);

	return appendFile(logFile, logContent);
}

int appendFile(const char *file, const char *content) {
	FILE *fp;

	if ((fp = fopen(file, "ab+")) == NULL) {
		printf("open file %s failed\n", file);
		return OTM_ERR;
	}

	fwrite(content, strlen(content), 1, fp);
	fclose(fp);

	return OTM_OK;
}

int writeFile(const char *file, const char *content) {
	FILE *fp;

	if ((fp = fopen(file, "wb")) == NULL) {
		printf("open file %s failed\n", file);
		return OTM_ERR;
	}

	fwrite(content, strlen(content), 1, fp);
	fclose(fp);

	return OTM_OK;
}

int killProcess(int pid) {
	int ret;

	ret = kill(pid, SIGTERM);
	if (0 != ret) {
		ret = kill(pid, SIGKILL);
		if (0 != ret) {
			writeLog(LOG_ERROR, "process %d:%d kill failed! error %s", getpid(),
					pid, strerror(errno));
		}
	}

	int status; //for waitpid
	pid_t ret2;
	time_t tt;
	time_t tt2;

	if (0 == ret) {
		time(&tt);
		ret2 = waitpid(pid, &status, WNOHANG);
		while (0 == ret2) {
			time(&tt2);
			if (tt2 - tt > 3) {
				break;
			}
			ret2 = waitpid(pid, &status, WNOHANG);
		}
		if (-1 == ret2) {
			writeLog(LOG_ERROR, "process %d recovery failed! error %s", pid,
					strerror(errno));
		}
	}

	return ret;
}

void sig_handler(int sig_no) {
	if (sig_no == SIGTERM) {
		quit_status = STOP_FORCE;
		writeLog(LOG_WARNING, "script will stoped with child process killed");
	} else if (sig_no == SIGINT) {
		quit_status = STOP_SAFE;
		writeLog(LOG_WARNING,
				"script will stoped after all child process quit");
	}
}

