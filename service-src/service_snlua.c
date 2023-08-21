#include "skynet.h"
#include "atomic.h"

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#if defined(__APPLE__)
#include <mach/task.h>
#include <mach/mach.h>
#endif

#define NANOSEC 1000000000
#define MICROSEC 1000000

// #define DEBUG_LOG

#define MEMORY_WARNING_REPORT (1024 * 1024 * 32)		// 32 MBytes

// 1.写lua代码
// 2.lua虚拟机词法分析、生成指令集 .byte
// 3.lua虚拟机执行指令集

/* snlua 是一切 lua 服务的原型，也是 99% 情况下业务中使用的服务 */
struct snlua {
	lua_State * L;					// lua 状态机（lua 虚拟机、沙盒环境）
	struct skynet_context * ctx;
	size_t mem;						// 已使用的内存大小		（字节）
	size_t mem_report;				// 触发警告的内存阀值 （每次触发阀值警告后，这个值会扩大2倍）
	size_t mem_limit;				// 内存使用上限
	lua_State * activeL;			// 目前正在运行的状态机（lua 协程）
	ATOM_INT trap;					// 打断状态（可以接受外部信号打断其运行状态）
}; // lua Actor 隔离环境

// LUA_CACHELIB may defined in patched lua for shared proto
#ifdef LUA_CACHELIB

#define codecache luaopen_cache

#else

static int
cleardummy(lua_State *L) {
  return 0;
}

static int 
codecache(lua_State *L) {
	luaL_Reg l[] = {
		{ "clear", cleardummy },
		{ "mode", cleardummy },
		{ NULL, NULL },
	};
	luaL_newlib(L,l);
	lua_getglobal(L, "loadfile");
	lua_setfield(L, -2, "loadfile");
	return 1;
}

#endif

static void
signal_hook(lua_State *L, lua_Debug *ar) {
	void *ud = NULL;
	lua_getallocf(L, &ud);			// 拿到创建 lua_State 的时候存进去的 snlua 结构体的指针
	struct snlua *l = (struct snlua *)ud;

	lua_sethook (L, NULL, 0, 0);	// 移除钩子函数
	if (ATOM_LOAD(&l->trap)) {
		ATOM_STORE(&l->trap , 0);	// 设置为可打断状态
		luaL_error(L, "signal 0");	// 通过报错打断当前执行逻辑
	}
}

static void
switchL(lua_State *L, struct snlua *l) {
	l->activeL = L;
	if (ATOM_LOAD(&l->trap)) {
		lua_sethook(L, signal_hook, LUA_MASKCOUNT, 1);
	}
}

static int
lua_resumeX(lua_State *L, lua_State *from, int nargs, int *nresults) {
	void *ud = NULL;
	lua_getallocf(L, &ud);
	struct snlua *l = (struct snlua *)ud;
	switchL(L, l);
	int err = lua_resume(L, from, nargs, nresults);
	if (ATOM_LOAD(&l->trap)) {
		// wait for lua_sethook. (l->trap == -1)
		while (ATOM_LOAD(&l->trap) >= 0) ;
	}
	switchL(from, l);
	return err;
}

static double
get_time() {
#if  !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ti);

	int sec = ti.tv_sec & 0xffff;
	int nsec = ti.tv_nsec;

	return (double)sec + (double)nsec / NANOSEC;
#else
	struct task_thread_times_info aTaskInfo;
	mach_msg_type_number_t aTaskInfoCount = TASK_THREAD_TIMES_INFO_COUNT;
	if (KERN_SUCCESS != task_info(mach_task_self(), TASK_THREAD_TIMES_INFO, (task_info_t )&aTaskInfo, &aTaskInfoCount)) {
		return 0;
	}

	int sec = aTaskInfo.user_time.seconds & 0xffff;
	int msec = aTaskInfo.user_time.microseconds;

	return (double)sec + (double)msec / MICROSEC;
#endif
}

static inline double
diff_time(double start) {
	double now = get_time();
	if (now < start) {
		return now + 0x10000 - start;
	} else {
		return now - start;
	}
}

// coroutine lib, add profile

/*
** Resumes a coroutine. Returns the number of results for non-error
** cases or -1 for errors.
*/
static int auxresume (lua_State *L, lua_State *co, int narg) {
  int status, nres;
  if (!lua_checkstack(co, narg)) {
    lua_pushliteral(L, "too many arguments to resume");
    return -1;  /* error flag */
  }
  lua_xmove(L, co, narg);
  status = lua_resumeX(co, L, narg, &nres);
  if (status == LUA_OK || status == LUA_YIELD) {
    if (!lua_checkstack(L, nres + 1)) {
      lua_pop(co, nres);  /* remove results anyway */
      lua_pushliteral(L, "too many results to resume");
      return -1;  /* error flag */
    }
    lua_xmove(co, L, nres);  /* move yielded values */
    return nres;
  }
  else {
    lua_xmove(co, L, 1);  /* move error message */
    return -1;  /* error flag */
  }
}

static int
timing_enable(lua_State *L, int co_index, lua_Number *start_time) {
	lua_pushvalue(L, co_index);
	lua_rawget(L, lua_upvalueindex(1));
	if (lua_isnil(L, -1)) {		// check total time
		lua_pop(L, 1);
		return 0;
	}
	*start_time = lua_tonumber(L, -1);
	lua_pop(L,1);
	return 1;
}

static double
timing_total(lua_State *L, int co_index) {
	lua_pushvalue(L, co_index);
	lua_rawget(L, lua_upvalueindex(2));
	double total_time = lua_tonumber(L, -1);
	lua_pop(L,1);
	return total_time;
}

static int
timing_resume(lua_State *L, int co_index, int n) {
	lua_State *co = lua_tothread(L, co_index);
	lua_Number start_time = 0;
	if (timing_enable(L, co_index, &start_time)) {
		start_time = get_time();
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] resume %lf\n", co, ti);
#endif
		lua_pushvalue(L, co_index);
		lua_pushnumber(L, start_time);
		lua_rawset(L, lua_upvalueindex(1));	// set start time
	}

	int r = auxresume(L, co, n);

	if (timing_enable(L, co_index, &start_time)) {
		double total_time = timing_total(L, co_index);
		double diff = diff_time(start_time);
		total_time += diff;
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] yield (%lf/%lf)\n", co, diff, total_time);
#endif
		lua_pushvalue(L, co_index);
		lua_pushnumber(L, total_time);
		lua_rawset(L, lua_upvalueindex(2));
	}

	return r;
}

static int luaB_coresume (lua_State *L) {
  luaL_checktype(L, 1, LUA_TTHREAD);
  int r = timing_resume(L, 1, lua_gettop(L) - 1);
  if (r < 0) {
    lua_pushboolean(L, 0);
    lua_insert(L, -2);
    return 2;  /* return false + error message */
  }
  else {
    lua_pushboolean(L, 1);
    lua_insert(L, -(r + 1));
    return r + 1;  /* return true + 'resume' returns */
  }
}

static int luaB_auxwrap (lua_State *L) {
  lua_State *co = lua_tothread(L, lua_upvalueindex(3));
  int r = timing_resume(L, lua_upvalueindex(3), lua_gettop(L));
  if (r < 0) {
    int stat = lua_status(co);
    if (stat != LUA_OK && stat != LUA_YIELD)
      lua_closethread(co, L);  /* close variables in case of errors */
    if (lua_type(L, -1) == LUA_TSTRING) {  /* error object is a string? */
      luaL_where(L, 1);  /* add extra info, if available */
      lua_insert(L, -2);
      lua_concat(L, 2);
    }
    return lua_error(L);  /* propagate error */
  }
  return r;
}

static int luaB_cocreate (lua_State *L) {
  lua_State *NL;
  luaL_checktype(L, 1, LUA_TFUNCTION);
  NL = lua_newthread(L);
  lua_pushvalue(L, 1);  /* move function to top */
  lua_xmove(L, NL, 1);  /* move function from L to NL */
  return 1;
}

static int luaB_cowrap (lua_State *L) {
  lua_pushvalue(L, lua_upvalueindex(1));
  lua_pushvalue(L, lua_upvalueindex(2));
  luaB_cocreate(L);
  lua_pushcclosure(L, luaB_auxwrap, 3);
  return 1;
}

// profile lib

static int
lstart(lua_State *L) {
	if (lua_gettop(L) != 0) {
		lua_settop(L,1);
		luaL_checktype(L, 1, LUA_TTHREAD);
	} else {
		lua_pushthread(L);
	}
	lua_Number start_time = 0;
	if (timing_enable(L, 1, &start_time)) {
		return luaL_error(L, "Thread %p start profile more than once", lua_topointer(L, 1));
	}

	// reset total time
	lua_pushvalue(L, 1);
	lua_pushnumber(L, 0);
	lua_rawset(L, lua_upvalueindex(2));

	// set start time
	lua_pushvalue(L, 1);
	start_time = get_time();
#ifdef DEBUG_LOG
	fprintf(stderr, "PROFILE [%p] start\n", L);
#endif
	lua_pushnumber(L, start_time);
	lua_rawset(L, lua_upvalueindex(1));

	return 0;
}

static int
lstop(lua_State *L) {
	if (lua_gettop(L) != 0) {
		lua_settop(L,1);
		luaL_checktype(L, 1, LUA_TTHREAD);
	} else {
		lua_pushthread(L);
	}
	lua_Number start_time = 0;
	if (!timing_enable(L, 1, &start_time)) {
		return luaL_error(L, "Call profile.start() before profile.stop()");
	}
	double ti = diff_time(start_time);
	double total_time = timing_total(L,1);

	lua_pushvalue(L, 1);	// push coroutine
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(1));

	lua_pushvalue(L, 1);	// push coroutine
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(2));

	total_time += ti;
	lua_pushnumber(L, total_time);
#ifdef DEBUG_LOG
	fprintf(stderr, "PROFILE [%p] stop (%lf/%lf)\n", lua_tothread(L,1), ti, total_time);
#endif

	return 1;
}

static int
init_profile(lua_State *L) {
	luaL_Reg l[] = {
		{ "start", lstart },	// start 和 stop 配合使用，可以获得时间间隔
		{ "stop", lstop },
		{ "resume", luaB_coresume },
		{ "wrap", luaB_cowrap },
		{ NULL, NULL },
	};

	// 通过 l 构建一个新的lua表，并返回其在lua栈中的索引
	luaL_newlibtable(L,l);
	
	// 创建3个空lua 表
	lua_newtable(L);	// table thread->start time
	lua_newtable(L);	// table thread->total time
	lua_newtable(L);	// weak table

	lua_pushliteral(L, "kv");
	lua_setfield(L, -2, "__mode");

	lua_pushvalue(L, -1);
	lua_setmetatable(L, -3);
	lua_setmetatable(L, -3);

	// 遍历 l 将这些函数注册到lua全局环境，将当前栈顶的 2 个值作为 upvalue 注册到新创建的闭包中
	luaL_setfuncs(L,l,2);

	return 1;
}

/// end of coroutine

static int 
traceback (lua_State *L) {
	const char *msg = lua_tostring(L, 1);
	if (msg)
		luaL_traceback(L, L, msg, 1);
	else {
		lua_pushliteral(L, "(no error message)");
	}
	return 1;
}

static void
report_launcher_error(struct skynet_context *ctx) {
	// sizeof "ERROR" == 5
	skynet_sendname(ctx, 0, ".launcher", PTYPE_TEXT, 0, "ERROR", 5);
}

static const char *
optstring(struct skynet_context *ctx, const char *key, const char * str) {
	const char * ret = skynet_command(ctx, "GETENV", key);
	if (ret == NULL) {
		return str;
	}
	return ret;
}

/// @brief 对 snlua 实例真正进行初始化的函数
/// @param l 
/// @param ctx 
/// @param args 
/// @param sz 
/// @return 
static int
init_cb(struct snlua *l, struct skynet_context *ctx, const char * args, size_t sz) {
	lua_State *L = l->L;
	l->ctx = ctx;
	lua_gc(L, LUA_GCSTOP, 0);		// 停止垃圾回收器

	// 将 lua 状态机的注册表中的 LUA_NOENV 变量 设置为 true （通过将LUA_NOENV设置为true，限制lua代码的访问范围，增加安全性）
	// 当LUA_NOENV = true时，lua的环境变量将不会被加载，即在执行lua代码时，无法通过环境变量访问到外部的全局变量、函数等。
	lua_pushboolean(L, 1);  						 // 向lua栈中压入一个 true
	lua_setfield(L, LUA_REGISTRYINDEX, "LUA_NOENV"); // pop 出栈顶的值设置指定位置的 LUA_NOENV 变量（LUA_REGISTRYINDEX 是指向lua注册表的特殊索引）

	// lua 状态机中加载 lua 标准库（基本数学库、表管理库、字符串处理库等）
	luaL_openlibs(L);

	// 加载一个 C 库并将其注册为一个名为 "skynet.profile" 的 lua 模块
	// 调用成功后，栈顶元素会有一个 table，即该模块的命名空间。这个 table 中包含了从 C 库导出的所有函数和全局变量。
	luaL_requiref(L, "skynet.profile", init_profile, 0);

	// 使用加载的 skynet.profile 库中的 resume 和 wrap 替换掉标准库协程中的同名函数
	int profile_lib = lua_gettop(L);
	// replace coroutine.resume / coroutine.wrap
	lua_getglobal(L, "coroutine");
	lua_getfield(L, profile_lib, "resume");
	lua_setfield(L, -2, "resume");
	lua_getfield(L, profile_lib, "wrap");
	lua_setfield(L, -2, "wrap");

	// 移除栈顶table（模块注册信息是保存在全局环境中，这里pop掉，注册的模块仍然生效）
	lua_settop(L, profile_lib-1);

	// 在 lua 状态机的注册表中新增 skynet_context 变量，其值为C层服务上下文的指针
	lua_pushlightuserdata(L, ctx);		// 压入轻量级用户数据，服务上下文指针
	lua_setfield(L, LUA_REGISTRYINDEX, "skynet_context");

	// 加载一个 C 库并将其注册为一个名为 "skynet.codecache" 的 lua 模块
	luaL_requiref(L, "skynet.codecache", codecache , 0);
	lua_pop(L,1);

	// 启动 Lua 的垃圾回收器执行一次增量式垃圾回收，并显式地触发老生代的垃圾回收（参数 `0` 表示对所有对象进行垃圾回收）
	lua_gc(L, LUA_GCGEN, 0, 0);

	// 设置相关配置的全局变量
	const char *path = optstring(ctx, "lua_path","./lualib/?.lua;./lualib/?/init.lua");
	lua_pushstring(L, path);
	lua_setglobal(L, "LUA_PATH");
	const char *cpath = optstring(ctx, "lua_cpath","./luaclib/?.so");
	lua_pushstring(L, cpath);
	lua_setglobal(L, "LUA_CPATH");
	const char *service = optstring(ctx, "luaservice", "./service/?.lua");
	lua_pushstring(L, service);
	lua_setglobal(L, "LUA_SERVICE");
	const char *preload = skynet_command(ctx, "GETENV", "preload");
	lua_pushstring(L, preload);
	lua_setglobal(L, "LUA_PRELOAD");

	lua_pushcfunction(L, traceback);
	assert(lua_gettop(L) == 1);

	// 加载 loader.lua 的代码，将其编译成 Lua 函数（loader 的作用是去各项代码目录查找指定的lua文件，找到后 loadfile 并执行(等效于 dofile)）
	const char * loader = optstring(ctx, "lualoader", "./lualib/loader.lua");
	int r = luaL_loadfile(L,loader);
	if (r != LUA_OK) {
		skynet_error(ctx, "Can't load %s : %s", loader, lua_tostring(L, -1));
		report_launcher_error(ctx);
		return 1;
	}

	// 通过 loader 加载指定的服务文件（args 即要查找并执行的lua文件名，如 "bootstrap"）
	lua_pushlstring(L, args, sz);
	r = lua_pcall(L,1,0,1);
	if (r != LUA_OK) {
		skynet_error(ctx, "lua loader error : %s", lua_tostring(L, -1));
		report_launcher_error(ctx);
		return 1;
	}
	lua_settop(L,0);

	// 检查注册表中是否有 lua 服务的内存上限变量，如果有则需要设置
	if (lua_getfield(L, LUA_REGISTRYINDEX, "memlimit") == LUA_TNUMBER) {
		size_t limit = lua_tointeger(L, -1);
		l->mem_limit = limit;
		skynet_error(ctx, "Set memory limit to %.2f M", (float)limit / (1024 * 1024));
		lua_pushnil(L);
		lua_setfield(L, LUA_REGISTRYINDEX, "memlimit");
	}
	lua_pop(L, 1);

	lua_gc(L, LUA_GCRESTART, 0);	// 重新启动垃圾回收器

	return 0;
}

static int
launch_cb(struct skynet_context * context, void *ud, int type, int session, uint32_t source , const void * msg, size_t sz) {
	assert(type == 0 && session == 0);
	struct snlua *l = ud;
	// 将服务原本绑定的句柄和回调函数清空（即注销C语言层面的回调函数，使它不再接收消息）
	skynet_callback(context, NULL, NULL);
	// 调用真正的初始化函数，控制权开始转到lua层
	int err = init_cb(l, context, msg, sz);
	if (err) {
		skynet_command(context, "EXIT", NULL);
	}

	return 0;
}

/// @brief 初始化 snlua 实例。不直接直接在这里做初始化，只是给自己发了一条消息，然后在消息的回调函数 launch_cb 里通过调用 init_cb 做实际的初始化工作
/// @param l 	snlua 沙盒环境
/// @param ctx 	要在该沙盒中运行的 lua 服务上下文
/// @param args 服务运行参数，如："bootstrap "
/// @return 
int
snlua_init(struct snlua *l, struct skynet_context *ctx, const char * args) {
	int sz = strlen(args);
	char * tmp = skynet_malloc(sz);
	memcpy(tmp, args, sz);
	// 注册回调函数 launch_cb，有消息传入时会调用回调函数并处理
	skynet_callback(ctx, l , launch_cb);
	const char * self = skynet_command(ctx, "REG", NULL);
	uint32_t handle_id = strtoul(self+1, NULL, 16);
	// it must be first message （给自己发送一条消息，内容是为args字符串）
	skynet_send(ctx, 0, handle_id, PTYPE_TAG_DONTCOPY,0, tmp, sz);
	return 0;
}

/// @brief 自定义的内存分配函数（主要负责处理 snlua 实例中跟内存有关的三个变量 mem / mem_limit / mem_report）
/// @param ud snlua 实例指针
/// @param ptr 
/// @param osize 
/// @param nsize 
/// @return 
static void *
lalloc(void * ud, void *ptr, size_t osize, size_t nsize) {
	struct snlua *l = ud;
	size_t mem = l->mem;
	l->mem += nsize;
	if (ptr)
		l->mem -= osize;
	if (l->mem_limit != 0 && l->mem > l->mem_limit) {
		if (ptr == NULL || nsize > osize) {
			l->mem = mem;
			return NULL;
		}
	}
	if (l->mem > l->mem_report) {
		l->mem_report *= 2;
		skynet_error(l->ctx, "Memory warning %.2f M", (float)l->mem / (1024 * 1024));
	}
	return skynet_lalloc(ptr, osize, nsize);
}

/// @brief 创建 snlua 实例
struct snlua *
snlua_create(void) {
	struct snlua * l = skynet_malloc(sizeof(*l));
	memset(l,0,sizeof(*l));
	l->mem_report = MEMORY_WARNING_REPORT;
	l->mem_limit = 0;
	l->L = lua_newstate(lalloc, l);	// 创建lua虚拟机，生成沙盒环境（使用自定义的内存分配方法）
	l->activeL = NULL;
	ATOM_INIT(&l->trap , 0);
	return l;
}

void
snlua_release(struct snlua *l) {
	lua_close(l->L);
	skynet_free(l);
}

/// @brief 信号中断，打断正在运行的脚本（这个功能主要是用来打断可能陷入死循环的服务）
/// @param l 
/// @param signal 
void
snlua_signal(struct snlua *l, int signal) {
	skynet_error(l->ctx, "recv a signal %d", signal);
	if (signal == 0) {
		if (ATOM_LOAD(&l->trap) == 0) {
			// only one thread can set trap ( l->trap 0->1 )
			if (!ATOM_CAS(&l->trap, 0, 1))
				return;

			// 设置钩子函数，每执行1条指令后调用钩子函数
			lua_sethook (l->activeL, signal_hook, LUA_MASKCOUNT, 1);
			// finish set ( l->trap 1 -> -1 )
			ATOM_CAS(&l->trap, 1, -1);
		}
	} else if (signal == 1) {
		skynet_error(l->ctx, "Current Memory %.3fK", (float)l->mem / 1024);
	}
}
