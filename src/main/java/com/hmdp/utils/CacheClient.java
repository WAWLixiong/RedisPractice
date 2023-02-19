package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        // 1. 查缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 命中返回
        // 2.1 不为空
        if (StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        // 2.2 为空但不为null
        if (json != null) {
            return null;
        }
        // key 不存在
        // 3. 未命中查数据库
        R r = dbFallback.apply(id);
        // 4. 数据库没有,返回错误信息
        if (r == null) {
            // 4.1 写入redis空值
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 4.2 返回错误信息
            return null;
        }
        // 5. 数据库有, 插入到缓存
        this.set(key, r, time, unit);
        // 6. 返回商品信息
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(String prefix, ID id, Class<R> type, Function<ID,R> dbFallBack, Long time, TimeUnit unit){
        String key = prefix + id;
        // 1. 查缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 未命中返回
        if (StrUtil.isBlank(json)){
            return null;
        }
        // 3. 命中先反序列化
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject)redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 3.1 未过期直接返回
        if (expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }
        // 3.2 过期加锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            // caution: double check确认数据是过期了, 因为可能刚重建的数据, 这里又拿到了锁
            // 3.3 拿到锁新开线程重建缓存并在建好后释放锁
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    R r1 = dbFallBack.apply(id);
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    unlock(lockKey);
                }
            });
        }
        // 3.4 获取不到锁返回旧的商品信息
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 30, TimeUnit.SECONDS);// 根据业务逻辑耗时决定
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
