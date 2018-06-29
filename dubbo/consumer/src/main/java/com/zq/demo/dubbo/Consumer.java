package com.zq.demo.dubbo;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Zhao Qing on 2018/6/29.
 */
public class Consumer {
    public static void main(String[] args) {
        //测试常规服务
        ClassPathXmlApplicationContext context =
                new ClassPathXmlApplicationContext("dubbo-demo-consumer.xml");
        context.start();
        System.out.println("consumer start");
        DemoService demoService = (DemoService) context.getBean("demoService");
        System.out.println("consumer");
        System.out.println(demoService.sayHello("赵庆"));
    }
}
