package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import com.hmdp.dto.UserDTO;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

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
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1. 校验手机号
        if (RegexUtils.isPhoneInvalid(phone)){
            // 2. 不符合，返回错误信息
            return Result.fail("手机号码格式错误!");
        }
        // 3. 生成验证码
        String code = RandomUtil.randomNumbers(6);
        // // 4. 保存到session
        // session.setAttribute("code", code);
        // 4. 保存到redis
        stringRedisTemplate.opsForValue().set("login:code:"+phone, code, 2, TimeUnit.MINUTES);
        // 5. 发送验证码
        log.debug("发送验证码成功, 验证码: {}", code);
        // 6. 返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1. 检验手机号和验证码
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)){
            // 2. 不符合，返回错误信息
            return Result.fail("手机号码格式错误!");
        }
        // // 3. 校验验证码
        // Object cacheCode = session.getAttribute("code");
        // 3. 校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get("login:code:"+phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            // 4. 验证码不正确报错
            return Result.fail("验证码不正确");
        }
        // 5. 查询用户 select * from tb_user where phone=
        User user = query().eq("phone", phone).one();

        // 6. 用户不存在创建用户
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        // // 7. 保存用户信息到session
        // session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        // 7. 保存到redis中
        // 7.1 生成token
        String token = UUID.randomUUID().toString();
        // 7.2 user转为hash
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 7.3 保存到redis
        Map<String, Object> stringObjectMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        stringRedisTemplate.opsForHash().putAll("login:token:"+token, stringObjectMap);
        stringRedisTemplate.expire("login:token:"+token, 30, TimeUnit.MINUTES);
        // 7.4 返回token(前端拿到token存到浏览器中)，后续请求会放在Authentication头部
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
