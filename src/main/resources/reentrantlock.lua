---
--- Generated by Luanalysis
--- Created by lixio.
--- DateTime: 2023-02-12 6:47
---

local key=KEYS[1]
local thread_id=ARGV[1]
local lease_time=ARGV[2] -- 毫秒


if (redis.call("exists", key) == 0) then
    redis.call("hset", key, thread_id, "1")
    redis.call("pexpire", key, lease_time)
    return 1
end

if (redis.call("hexists", key, thread_id) == 1) then
    redis.call("hincr", key, thread_id, "1")
    redis.call("pexpire", key, thread_id, lease_time)
    return 1
end

return 0
