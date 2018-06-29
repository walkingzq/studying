package com.zq.demo.dubbo.provider;

import com.zq.demo.dubbo.DemoService;

/**
 * Created by Zhao Qing on 2018/6/29.
 */
public class DemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        System.out.println("用户姓名：" + name);
        return "hello," + name;
    }
}
