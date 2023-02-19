package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LoginInterceptor implements HandlerInterceptor {
    private StringRedisTemplate stringRedisTemplate;

    public LoginInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // // 1. 获取session
        // HttpSession session = request.getSession();
        // // 2. 获取session的用户
        // Object user = session.getAttribute("user");

        // 1. 获取头部的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            response.setStatus(401);
            return false;
        }
        // 2. 基于token从redis获取用户
        Map<Object, Object> user = stringRedisTemplate.opsForHash().entries("login:token:" + token);
        // 3. 判断用户是否存在
        if (user.isEmpty()) {
            // 4. 不存在, 拦截
            response.setStatus(401);
            return false;
        }
        // 4. 将hash对象转为UserDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(user, new UserDTO(), false);
        // 5. 存在，保存在ThreadLocal
        UserHolder.saveUser(userDTO);
        // 6. 刷新Token
        stringRedisTemplate.expire("login:token:" + token, 30, TimeUnit.MINUTES);
        // 7. 放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
