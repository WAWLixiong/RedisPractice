package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component // 定义为Bean
public class RedisIdWorker {
    // 开始时间时间戳
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 一位符号位，31位时间戳, 32位序列号
     * @param keyPrefix 业务前缀
     * @return 全局唯一id
     */
    public long nextId(String keyPrefix){
        // 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        // 生成自增长序列, 不用担心空指针, redis在没有的键上increase会创建
        long count = stringRedisTemplate.opsForValue().increment("inc:" + keyPrefix + ":" + date);
        // 拼接并返回
        return timestamp<<32|count;
    }
}
