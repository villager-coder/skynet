#include "skynet.h"
#include "skynet_server.h"
#include "skynet_imp.h"
#include "skynet_mq.h"
#include "skynet_handle.h"
#include "skynet_module.h"
#include "skynet_timer.h"
#include "skynet_monitor.h"
#include "skynet_socket.h"
#include "skynet_daemon.h"
#include "skynet_harbor.h"

#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

/* 监控器，用于监控所有worker线程状态的结构体 */
struct monitor {
	int count;						// skynet的worker线程总量
	struct skynet_monitor ** m;		// 次级监控器列表，一个监控器监控一个worker线程
	pthread_cond_t cond;			// 唤醒 worker 线程用的全局条件
	pthread_mutex_t mutex;
	int sleep;						// 睡眠中的线程数数量
	int quit;						// 退出标记
};
// 和 monitor 线程同名，但该结构体并不是 monitor 线程的专用数据，而是给所有需要调度 worker 线程的线程使用的。

/* worker 线程入口函数的传入参数 */
struct worker_parm {
	struct monitor *m;		// 全局的monitor实例地址（一个skynet进程只有一个monitor实例，由主线程管理其生命周期）
	int id;					// worker 线程id
	int weight;				// worker 线程每次处理的工作量权重
};
// 每个 worker 线程都有一个属于自己的参数结构体 worker_parm，用来保存一些本线程的参数。

static volatile int SIG = 0;

static void
handle_hup(int signal) {
	if (signal == SIGHUP) {
		SIG = 1;
	}
}

#define CHECK_ABORT if (skynet_context_total()==0) break;

static void
create_thread(pthread_t *thread, void *(*start_routine) (void *), void *arg) {
	if (pthread_create(thread,NULL, start_routine, arg)) {
		fprintf(stderr, "Create thread failed");
		exit(1);
	}
}

static void
wakeup(struct monitor *m, int busy) {
	if (m->sleep >= m->count - busy) {
		// signal sleep worker, "spurious wakeup" is harmless
		pthread_cond_signal(&m->cond);
	}
}

static void *
thread_socket(void *p) {
	struct monitor * m = p;
	skynet_initthread(THREAD_SOCKET);
	for (;;) {
		int r = skynet_socket_poll();
		if (r==0)
			break;
		if (r<0) {
			CHECK_ABORT
			continue;	// 一般是还有消息没处理完，直接继续循环
		}
		wakeup(m,0);	// 尝试唤醒一个 worker 线程（只有全部worker线程都处于睡眠状态才回去唤醒，否则不做处理）
	}
	return NULL;
}

static void
free_monitor(struct monitor *m) {
	int i;
	int n = m->count;
	for (i=0;i<n;i++) {
		skynet_monitor_delete(m->m[i]);
	}
	pthread_mutex_destroy(&m->mutex);
	pthread_cond_destroy(&m->cond);
	skynet_free(m->m);
	skynet_free(m);
}

static void *
thread_monitor(void *p) {
	struct monitor * m = p;
	int i;
	int n = m->count;						// 拿到 worker 线程的数量
	skynet_initthread(THREAD_MONITOR);		// 设置线程属性
	for (;;) {
		CHECK_ABORT
		// 遍历全部 worker 线程的子监控器，检查线程状态，状态异常则输出日志
		for (i=0;i<n;i++) {
			skynet_monitor_check(m->m[i]);
		}

		// 睡眠 5s
        // 使用循环分开调用是为了更快的触发 abort
		for (i=0;i<5;i++) {
			CHECK_ABORT
			sleep(1);
		}
	}

	return NULL;
}

static void
signal_hup() {
	// make log file reopen

	struct skynet_message smsg;
	smsg.source = 0;
	smsg.session = 0;
	smsg.data = NULL;
	smsg.sz = (size_t)PTYPE_SYSTEM << MESSAGE_TYPE_SHIFT;
	uint32_t logger = skynet_handle_findname("logger");
	if (logger) {
		skynet_context_push(logger, &smsg);
	}
}

static void *
thread_timer(void *p) {
	struct monitor * m = p;
	skynet_initthread(THREAD_TIMER);
	for (;;) {
		skynet_updatetime();
		skynet_socket_updatetime();
		CHECK_ABORT
		wakeup(m,m->count-1);
		usleep(2500);			// 睡眠2.5ms
		if (SIG) {
			signal_hup();
			SIG = 0;
		}
	}
	// wakeup socket thread
	skynet_socket_exit();
	// wakeup all worker thread
	pthread_mutex_lock(&m->mutex);
	m->quit = 1;
	pthread_cond_broadcast(&m->cond);
	pthread_mutex_unlock(&m->mutex);
	return NULL;
}

static void *
thread_worker(void *p) {
	struct worker_parm *wp = p;
	int id = wp->id;
	int weight = wp->weight;
	struct monitor *m = wp->m;
	struct skynet_monitor *sm = m->m[id];
	skynet_initthread(THREAD_WORKER);
	struct message_queue * q = NULL;
	while (!m->quit) {
		q = skynet_context_message_dispatch(sm, q, weight);	// 从全局队列中取weight个数量的消息进程处理
		if (q == NULL) {
			// 全局队列中暂时没有待处理的消息队列，休眠本worker线程
			if (pthread_mutex_lock(&m->mutex) == 0) {
				++ m->sleep;
				// "spurious wakeup" is harmless,
				// because skynet_context_message_dispatch() can be call at any time.
				if (!m->quit)
					pthread_cond_wait(&m->cond, &m->mutex);		// 等待被其他线程唤醒（一般情况是timer线程和socket线程）
				-- m->sleep;
				if (pthread_mutex_unlock(&m->mutex)) {
					fprintf(stderr, "unlock mutex error");
					exit(1);
				}
			}
		}
	}
	return NULL;
}

/// @brief 启动全部线程
/// @param thread worker线程数量
static void
start(int thread) {
	pthread_t pid[thread+3];

	struct monitor *m = skynet_malloc(sizeof(*m));
	memset(m, 0, sizeof(*m));
	m->count = thread;
	m->sleep = 0;

	// 每条 worker 线程分配一个 skynet_monitor 监控
	m->m = skynet_malloc(thread * sizeof(struct skynet_monitor *));
	int i;
	for (i=0;i<thread;i++) {
		m->m[i] = skynet_monitor_new();
	}
	if (pthread_mutex_init(&m->mutex, NULL)) {
		fprintf(stderr, "Init mutex error");
		exit(1);
	}
	if (pthread_cond_init(&m->cond, NULL)) {
		fprintf(stderr, "Init cond error");
		exit(1);
	}

	create_thread(&pid[0], thread_monitor, m);	// 过载线程 1个
	create_thread(&pid[1], thread_timer, m);	// 定时器线程 1个
	create_thread(&pid[2], thread_socket, m);	// socket线程 1个

	// worker 线程每次处理的工作量权重（是服务队列中消息总数右移的位数，小于 0 的每次只读一条）\
	 前四个线程每次只处理一条消息 \
     后面的四个每次处理队列中的全部消息 \
     再后面分别是每次 1/2，1/4，1/8
	// 不同权重的目的是为了尽量让不同的 worker 线程的步骤不一样，从而减轻在全局消息队列那里的锁竞争问题
	static int weight[] = { 
		-1, -1, -1, -1, 0, 0, 0, 0,
		1, 1, 1, 1, 1, 1, 1, 1, 
		2, 2, 2, 2, 2, 2, 2, 2, 
		3, 3, 3, 3, 3, 3, 3, 3, };
	struct worker_parm wp[thread];
	for (i=0;i<thread;i++) {
		wp[i].m = m;
		wp[i].id = i;
		if (i < sizeof(weight)/sizeof(weight[0])) {
			wp[i].weight= weight[i];
		} else {
			wp[i].weight = 0;
		}
		create_thread(&pid[i+3], thread_worker, &wp[i]);	// worker线程 （业务线程）
	}

	for (i=0;i<thread+3;i++) {
		pthread_join(pid[i], NULL); 		// 主线程阻塞
	}

	free_monitor(m);
}

/// @brief 加载 bootstrap 引导模块
/// @param logger 如果加载出错，用于输出的日志服务上下文
/// @param cmdline 加载命令，例如："snlua bootstrap"，将会加载并运行service/bootstrap.lua
static void
bootstrap(struct skynet_context * logger, const char * cmdline) {
	int sz = strlen(cmdline);
	char name[sz+1];
	char args[sz+1];
	int arg_pos;
	sscanf(cmdline, "%s", name);  
	arg_pos = strlen(name);
	if (arg_pos < sz) {
		while(cmdline[arg_pos] == ' ') {
			arg_pos++;
		}
		strncpy(args, cmdline + arg_pos, sz);
	} else {
		args[0] = '\0';
	}

	/* skynet_context_new("snlua", "bootstrap");
	 -> 加载 snlua.so 模块，实例化一个 snlua 服务，根据传入的要实例化的lua服务的脚本名称去获取lua脚本，
	 这里是 "bootstarp"，则会查找配置的luaservice目录中的bootstrap.lua，如果是默认配置，将会找到service/bootstrap.lua 
	 snlua 是lua的沙盒服务，所有的 lua 服务 都是一个 snlua 的实例 */ 
	struct skynet_context *ctx = skynet_context_new(name, args);
	if (ctx == NULL) {
		skynet_error(NULL, "Bootstrap error : %s\n", cmdline);
		skynet_context_dispatchall(logger);
		exit(1);
	}
}

void 
skynet_start(struct skynet_config * config) {
	// register SIGHUP for log file reopen
	struct sigaction sa;
	sa.sa_handler = &handle_hup;
	sa.sa_flags = SA_RESTART;
	sigfillset(&sa.sa_mask);
	sigaction(SIGHUP, &sa, NULL);

	if (config->daemon) {
		// 初始化守护进程
		if (daemon_init(config->daemon)) {
			exit(1);
		}
	}

	// 初始化节点模块，用于集群，转发远程节点的消息
	skynet_harbor_init(config->harbor);

	// 初始化 服务handle 存储管理器，用于给每个Skynet服务创建一个全局唯一的句柄值
	skynet_handle_init(config->harbor);

	// 初始化全局消息队列
	skynet_mq_init();

	// 初始化 C 模块管理器，设置查找路径，主要用于加载符合Skynet服务模块接口的动态链接库（.so
	skynet_module_init(config->module_path);

	// 初始化定时器模块（全局时间）
	skynet_timer_init();

	// 初始化网络模块（socket管理器）
	skynet_socket_init();

	// 标记是否开了性能测试
	skynet_profile_enable(config->profile);

	// 创建并启动 logger C服务
	struct skynet_context *ctx = skynet_context_new(config->logservice, config->logger);
	if (ctx == NULL) {
		fprintf(stderr, "Can't launch %s service\n", config->logservice);
		exit(1);
	}

	// 注册 logger 服务的句柄名称，注册后即可通过"logger"拿到logger C服务的上下文句柄
	skynet_handle_namehandle(skynet_context_handle(ctx), "logger");

	// 加载 bootstrap 引导模块，如果使用默认 config 的配置内容，config->bootstrap 的内容是 "snlua bootstrap"
	bootstrap(ctx, config->bootstrap);

	// 启动全部线程
	start(config->thread);

	// harbor_exit may call socket send, so it should exit before socket_free
	skynet_harbor_exit();
	skynet_socket_free();
	if (config->daemon) {
		daemon_exit(config->daemon);
	}
}
