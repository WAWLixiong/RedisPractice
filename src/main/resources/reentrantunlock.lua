---
--- Generated by Luanalysis
--- Created by lixio.
--- DateTime: 2023-02-12 6:56
---

local key=KEYS[1]
local thread_id=ARGV[1]
local lease_time=ARGV[2] -- 毫秒

if(redis.call("hexists", key, thread_id) ~= thread_id) then
    return nil
end;

local value = redis.call("hincrby", key, thread_id, "-1")
if (value > 0) then
    redis.call("pexpire", key, lease_time)
    return nil
else
    redis.call("del", key)
    return nil
end