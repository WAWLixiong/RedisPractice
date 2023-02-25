package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    IUserService iUserService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 1.判断目前是否关注
        UserDTO user = UserHolder.getUser();
        // 2. 做修改
        if (isFollow) {
            Follow follow = new Follow();
            follow.setUserId(user.getId());
            follow.setFollowUserId(followUserId);
            boolean save = save(follow);
            if (save) {
                // 放入关注集合
                String key = "follows:" + user.getId();
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            boolean remove = remove(new QueryWrapper<Follow>()
                    .eq("user_id", user.getId())
                    .eq("follow_user_id", followUserId)
            );
            if (remove){
                // 从关注集合删除
                String key = "follows:" + user.getId();
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        // 1. 查询是否关注
        UserDTO user = UserHolder.getUser();
        Integer count = query().eq("user_id", user.getId()).eq("follow_user_id", followUserId).count();
        return Result.ok(count > 0 );
    }

    @Override
    public Result followCommons(Long id) {
        // 当前用户
        UserDTO user = UserHolder.getUser();
        String key = "follows:" + user.getId();
        String key2 = "follows:" +id;
        // 求交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null || intersect.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        // 解析id
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        // 查询用户信息
        List<Object> userDTOs = iUserService.listByIds(ids)
                .stream().map(u -> BeanUtil.copyProperties(u, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOs);
    }
}
