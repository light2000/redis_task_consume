/*
 * bj.h
 *
 *  Created on: Oct 2, 2017
 *      Author: light2000
 */

#ifndef OTM_TASK_MONITOR_H_
#define OTM_TASK_MONITOR_H_

#define OTM_DEBUG 0
/**
 * log level
 */
#define LOG_ERROR 1
#define LOG_WARNING 2
#define LOG_NOTICE 8
#define LOG_DEBUG 16

#define OTM_ERR -1
#define OTM_OK 0

/**
 * script parameters number
 */
#define MAX_PARA_NUMS      10
/**
 * per script parameter length
 */
#define MAX_CHAR_EACH_PARA 50

/**
 * max child process number
 */
#define MAX_CHILD_PROCESS_NUMS 100

/**
 * max command script length
 */
#define MAX_CMD_LENTH 600

/**
 * command redis status
 */
#define STATUS_ERR "2"
#define STATUS_END "3"
#define STATUS_LAUNCHED "1"

/**
 * script info key in redis
 */
#define TASK_RUN_SET "otm_task_running"

#define TASK_QUEUE_NAME "otm_task_queue"

#define TASK_KILLED_SET "otm_task_killed"

#define TASK_FINISHED_SET "otm_task_finished"

#define TASK_SETTING_HSET "otm_task_setting"

#define TASK_WAITTING_HSET "otm_task_waiting"

#define REDIS_RETRY_TIMES 3

#define PROJECT_NAME "otm-task-monitor"

#define STOP_FORCE 2

#define STOP_SAFE 1

int main(int argc, char *argv[]);

/**
 * script redis data op functions
 */

void initOtm();
int initTask(int processId, const char *script, const char *taskId);
int getTaskLivetime(int processId);
/**
 * hiredis api encapsulation
 */
redisContext* buildRedisContext(int sleepSecond);
int hsetRedis(const char *key, const char *field, const char *value, int nx,
		int retryTimes);
int hdelRedis(const char *key, const char *field, int retryTimes);
int hgetRedis(const char *key, const char *field, char *result, int resultLen, int retryTimes);
int pushRedis(const char *queueName, const char *value, int retryTimes);
int popRedis(const char *queueName, char *result, int resultSize);
int saddRedis(const char *setName, const char *value, int retryTimes);
int sremRedis(const char *setName, const char *value, int retryTimes);

/**
 * task process handler
 */
int forkRun(char *cmd, char *taskKey);
int killProcess(int pid);

/**
 * util
 */
int split(char *input, char output[MAX_PARA_NUMS][MAX_CHAR_EACH_PARA]);
int shellExc(const char *command);
int writeLog(int logLevel, const char *logFormat, ...);
int appendFile(const char *file, const char *content);
int writeFile(const char *file, const char *content);
void sig_handler(int sig_no);
#endif /* OTM_TASK_MONITOR_H_ */
