package com.zq.demo.dubbo.provider;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * Created by Zhao Qing on 2018/6/29.
 */
public class Provider {
    public static void main(String[] args) throws IOException{
        System.setProperty("java.net.preferIPv4Stack", "true");
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"dubbo-demo-provider.xml"});
        System.out.println("lala");
        context.start();
        System.out.println("服务已经启动...");
        System.in.read();
    }
}
