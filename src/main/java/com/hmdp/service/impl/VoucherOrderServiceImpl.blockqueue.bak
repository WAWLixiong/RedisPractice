package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService iSeckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private IVoucherOrderService proxy;

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true) {
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 生成订单信息记录到库
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    // 处理订单异常
                    log.error("处理订单异常", e);
                }

            }

        }

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
            // 使用 redisson的工具
            // 兜底: 防止redis意外了
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            boolean locked = lock.tryLock();
            if (!locked) {
                return;
            }
            try {
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * 同步订单处理流程
     * @param voucherId
     * @return
     */
    public Result seckillVoucherOld(Long voucherId) {
        // 查询优惠券
        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
        // 大于开始时间
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        // 小于结束时间
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }
        // 库存大于0
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }
        // sprint事务失效，代理对象与真实对象的问题，synchronized锁加在代理对象上事务才能生效
        // Long userId = UserHolder.getUser().getId();
        // synchronized (userId.toString().intern()){
        //     IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
        //     return proxy.createVoucherOrder(voucherId);
        // }


        Long userId = UserHolder.getUser().getId();
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        // 使用 redisson的工具
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean locked = lock.tryLock();
        if (!locked) {
            return Result.fail("不允许重复下单");
        }
        try {
            // return proxy.createVoucherOrderOld(voucherId);
            return Result.fail("同步订单需要处理上边那行代码，并更改接口类型为Result类型");
        } finally {
            lock.unlock();
        }
    }

    @Transactional
    public Result createVoucherOrderOld(Long voucherId) {
        // 一人一单
        Long userId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        System.out.println("count:" + count);
        if (count > 0) {
            return Result.fail("用户已经购买过");
        }

        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                // .eq("stock", voucher.getStock()) // gt 0 更合理，避免更多用户失败
                .gt("stock", 0)
                .update();

        // 生成订单
        if (!success) {
            return Result.fail("库存不足");
        }
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        // 写入数据库
        save(voucherOrder);
        // 返回订单id
        return Result.ok(orderId);
    }

    /**
     * 异步订单处理流程, 可以在实际使用中先不使用锁生成订单，有异常时再分析确认加锁的必要行
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单
        Long userId = voucherOrder.getUserId();
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        System.out.println("count:" + count);
        if (count > 0) {
            log.error("用户已经购买过");
            return;
        }
        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                // .eq("stock", voucher.getStock()) // gt 0 更合理，避免更多用户失败
                .gt("stock", 0)
                .update();
        // 生成订单
        if (!success) {
            log.error("库存不足");
            return;
        }
        // 写入数据库
        save(voucherOrder);
    }
    private static final DefaultRedisScript<Long> ORDER_SCRIPT;
    // static内代码在类加载时执行一次
    static {
        ORDER_SCRIPT = new DefaultRedisScript<>();
        ORDER_SCRIPT.setLocation(new ClassPathResource("order.lua"));
        ORDER_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
        Long userId = UserHolder.getUser().getId();
        Long status = stringRedisTemplate.execute(
                ORDER_SCRIPT,
                Collections.emptyList(),
                voucher.getVoucherId().toString(),
                userId.toString(),
                String.valueOf(System.currentTimeMillis() / 1000)
        );
        if (status != 0) {
            String desc;
            if (status == 1){
                desc = "商品库存不足";
            } else if (status == 2) {
                desc = "活动未开始";
            } else if (status == 3) {
                desc = "优惠券已过期";
            } else if (status == 4) {
                desc = "不能重复下单";
            } else {
                desc = "未知原因错误";
            }
            return Result.fail(desc);
        }
        long orderId = redisIdWorker.nextId("order");

        // 生成订单并放入队列
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        orderTasks.add(voucherOrder);

        // 更新proxy对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 返回订单id
        return Result.ok(orderId);
    }
}
