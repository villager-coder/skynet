local skynet = require "skynet"
local harbor = require "skynet.harbor"
local service = require "skynet.service"
require "skynet.manager"	-- import skynet.launch, ...

skynet.start(function()
	-- 根据 standalone 配置决定启动的是一个 master 节点还是 slave 节点
	local standalone = skynet.getenv "standalone"

	-- 启动 launcher 服务
	-- 主要任务是作为启动器，并额外向 debug 后台提供一些针对服务的操作接口。比如执行 GC, 查看内存，杀死某个服务
	local launcher = assert(skynet.launch("snlua","launcher"))
	skynet.name(".launcher", launcher)

	-- 读取 harbor 配置处理 harbor 模式
	-- 没有使用 harbor 则单节点模式，启动一个 cdummy 服务负责拦截对外广播的全局名字变更
	-- 使用了 harbor 则多节点模式，主节点启动 cmaster 服务，包括主节点在内所有节点都启动 cslave 服务
	local harbor_id = tonumber(skynet.getenv "harbor" or 0)
	if harbor_id == 0 then
		assert(standalone ==  nil)
		standalone = true
		skynet.setenv("standalone", "true")

		local ok, slave = pcall(skynet.newservice, "cdummy")
		if not ok then
			skynet.abort()
		end
		skynet.name(".cslave", slave)

	else
		if standalone then
			if not pcall(skynet.newservice,"cmaster") then	-- cmaster服务提供节点调度
				skynet.abort()
			end
		end

		local ok, slave = pcall(skynet.newservice, "cslave") -- cslave 服务提供节点间的消息转发，以及同步全局名字
		if not ok then
			skynet.abort()
		end
		skynet.name(".cslave", slave)
	end

	if standalone then
		-- master 节点，需要启动 datacenterd 服务
		local datacenter = skynet.newservice "datacenterd"
		skynet.name("DATACENTER", datacenter)
	end
	-- 启动用于 UniqueService 管理的 service_mgr 服务，提供全局服务的名字查询和全局服务创建的功能
	skynet.newservice "service_mgr"

	local enablessl = skynet.getenv "enablessl"
	if enablessl then
		service.new("ltls_holder", function ()
			local c = require "ltls.init.c"
			c.constructor()
		end)
	end

	-- 根据配置文件中的start配置项启动用户自定义的lua脚本服务，假如无此项配置，则启动main.lua
	pcall(skynet.newservice,skynet.getenv "start" or "main")
	-- 退出bootstrap服务
	skynet.exit()
end)
