#ifndef SKYNET_MODULE_H
#define SKYNET_MODULE_H

struct skynet_context;

typedef void * (*skynet_dl_create)(void);
typedef int (*skynet_dl_init)(void * inst, struct skynet_context *, const char * parm);
typedef void (*skynet_dl_release)(void * inst);
typedef void (*skynet_dl_signal)(void * inst, int signal);

/* skynet 中的 module，用于保存加载完成的.so库信息 */
struct skynet_module {
	const char * name;			// 模块的名字（对应.so的名字）xxx
	void * module;				// 模块的地址（加载动态链接库获得的句柄）
	skynet_dl_create create;	// 库中create函数地址		xxx_create		用于创建一份该module的实例
	skynet_dl_init init;		// 库中init函数地址 		xxx_init		初始化create创建出来的私有数据
	skynet_dl_release release;	// 库中release函数地址 		xxx_release		在服务退出的时候被调用，用来释放create创建出来的私有数据
	skynet_dl_signal signal;	// 库中signal函数地址 		xxx_signal		用来接收 debug 控制台的信号指令
};

struct skynet_module * skynet_module_query(const char * name);
void * skynet_module_instance_create(struct skynet_module *);
int skynet_module_instance_init(struct skynet_module *, void * inst, struct skynet_context *ctx, const char * parm);
void skynet_module_instance_release(struct skynet_module *, void *inst);
void skynet_module_instance_signal(struct skynet_module *, void *inst, int signal);

void skynet_module_init(const char *path);

#endif
