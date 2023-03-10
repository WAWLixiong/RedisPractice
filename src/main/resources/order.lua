---
--- Generated by Luanalysis
--- Created by lixio.
--- DateTime: 2023-02-12 23:18
---

local voucher_id = ARGV[1]
local user_id = ARGV[2]
local current_time = ARGV[3]
local order_id = ARGV[4]

local voucher_id_key = "voucher:" .. voucher_id
local order_key = "voucher:order"

-- 库存不足
if (tonumber(redis.call("hget", voucher_id_key, "stock")) <= 0) then
    return 1
end

-- 时间不满足
if (tonumber(redis.call("hget", voucher_id_key, "start_time")) > tonumber(current_time)) then
    return 2
end

if (tonumber(redis.call("hget", voucher_id_key, "end_time")) < tonumber(current_time)) then
    return 3
end

-- 已经下过单
if (redis.call("sismember", order_key, user_id) == 1) then
    return 4
end

-- 下单
redis.call("hincrby", voucher_id_key, "stock", -1)
redis.call("sadd", order_key, user_id)
-- 生成订单到队列
redis.call("xadd", "stream.orders", "*", "id", order_id, "userId", user_id, "voucherId", voucher_id)
return 0
