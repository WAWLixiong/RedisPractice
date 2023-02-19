package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import io.netty.util.internal.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    public Result queryById(Long id) {
        // 缓存穿透
        // Shop shop = queryWithPassThrough(id);
        // Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 缓存击穿
        // 方式一: 加锁，使后续请求同步
        // Shop shop = queryWithMutex(id);
        // 方式二: 牺牲一致性
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

//    public Shop queryWithMutex(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        // 1. 查缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2. 命中返回
//        // 2.1 不为空
//        if (StrUtil.isNotBlank(shopJson)){
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // 2.2 为空但不为null
//        if (shopJson != null) {
//            return null;
//        }
//        // 3. key 不存在, 缓存重建
//        // 3.1 获取锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            boolean isLocked = tryLock(lockKey);
//            // 3.2 获取失败，休眠重试
//            if (!isLocked) {
//                Thread.sleep(50);
//                return queryWithPassThrough(id);
//            }
//            // 3.3 成功
//            // 3. 未命中查数据库
//            shop = getById(id);
//            Thread.sleep(200);
//            // 4. 数据库没有,返回错误信息
//            if (shop == null) {
//                // 4.1 写入redis空值
//                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//                // 4.2 返回错误信息
//                return null;
//            }
//            // 5. 数据库有, 插入到缓存
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            // 6. 释放锁
//            unlock(lockKey);
//        }
//        // 7. 返回商品信息
//        return shop;
//    }

//    public Shop queryWithPassThrough(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        // 1. 查缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2. 命中返回
//        // 2.1 不为空
//        if (StrUtil.isNotBlank(shopJson)){
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // 2.2 为空但不为null
//        if (shopJson != null) {
//            return null;
//        }
//        // key 不存在
//        // 3. 未命中查数据库
//        Shop shop = getById(id);
//        // 4. 数据库没有,返回错误信息
//        if (shop == null) {
//            // 4.1 写入redis空值
//            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//            // 4.2 返回错误信息
//            return null;
//        }
//        // 5. 数据库有, 插入到缓存
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        // 6. 返回商品信息
//        return shop;
//
//    }

//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

//    public Shop queryWithLogicalExpire(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        // 1. 查缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2. 未命中返回
//        if (StrUtil.isBlank(shopJson)){
//            return null;
//        }
//        // 3. 命中先反序列化
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject)redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        // 3.1 未过期直接返回
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            return shop;
//        }
//        // 3.2 过期加锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        if (isLock) {
//            // caution: double check确认数据是过期了, 因为可能刚重建的数据, 这里又拿到了锁
//            // 3.3 拿到锁新开线程重建缓存并在建好后释放锁
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try {
//                    saveShop2Redis(id, 20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//                finally {
//                    unlock(lockKey);
//                }
//            });
//        }
//        // 3.4 获取不到锁返回旧的商品信息
//        return shop;
//    }

//    private boolean tryLock(String key){
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 30, TimeUnit.SECONDS);// 根据业务逻辑耗时决定
//        return BooleanUtil.isTrue(flag);
//    }

//    private void unlock(String key){
//        stringRedisTemplate.delete(key);
//    }

    @Override
    @Transactional // TODO: 这里的事务无法回滚 REDIS
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1. 更新数据库
        updateById(shop);
        // 2. 删缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

//    public void saveShop2Redis(Long id, Long expireSeconds){
//        // 1. 查询
//        Shop shop = getById(id);
//        try {
//            Thread.sleep(200);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        // 2. 封装为逻辑过期数据
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//        // 3. 写入redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id, JSONUtil.toJsonStr(redisData));
//    }

}
