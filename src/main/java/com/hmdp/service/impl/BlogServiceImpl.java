package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.var;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current){
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogById(Long id) {
        // 查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("blog not exists");
        }
        // 查询用户
        queryBlogUser(blog);
        // 查询是否点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 1. 获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，默认显示未点赞
            return;
        }
        Long userId = user.getId();
        String key = BLOG_LIKED_KEY + blog.getId();
        // Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }


    @Override
    public Result likeBlog(Long id) {
        // 1. 获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 2. 判断是否点赞过
        String key = BLOG_LIKED_KEY +id;
        // Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        // if (BooleanUtil.isFalse(isMember)){
        if (score == null){
            // 3. 未点赞, 则赞
            // 3.1 数据库+1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            // 3.2 保存用户到redis集合
            if (isSuccess) {
                // stringRedisTemplate.opsForSet().add(key, userId.toString());
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else{
            // 4.已点赞, 则取消赞
            // 4.1 数据库-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            // 4.2 用户从redis集合删除
            if (isSuccess) {
                // stringRedisTemplate.opsForSet().remove(key, userId.toString());
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }

        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikesById(Long id) {
        String key = BLOG_LIKED_KEY +id;
        // 查top5用户id
        Set<String> userIds = stringRedisTemplate.opsForZSet().range(key, 0, 5);
        if (userIds == null || userIds.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = userIds.stream().map(Long::valueOf).collect(Collectors.toList());
        String idsStr = StrUtil.join(",", ids);
        // 通过用户id查用户信息
        // List<UserDTO> userDTOS = userService.listByIds(ids)
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids)
                .last("ORDER BY FIELD(id," + idsStr + ")")
                .list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
