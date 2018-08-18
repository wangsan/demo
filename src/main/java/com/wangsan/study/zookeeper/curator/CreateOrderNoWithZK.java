package com.wangsan.study.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * Created by zhuzs on 2017/5/4.
 */
public class CreateOrderNoWithZK {

    private static final String path = "/lock_path";

    public static void main(String[] args) {

        CuratorFramework client = getClient();
        final InterProcessMutex lock = new InterProcessMutex(client, path);
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final long startTime = new Date().getTime();
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        countDownLatch.await();
                        lock.acquire();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss|SSS");
                    System.out.println(sdf.format(new Date()));

                    try {
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("显示此线程大概花费时间（等待+执行）:" + (new Date().getTime() - startTime) + "ms");
                }
            }).start();
        }
        System.out.println("创建线程花费时间:" + (new Date().getTime() - startTime) + "ms");
        countDownLatch.countDown();
    }

    private static CuratorFramework getClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(3000)
                .namespace("demo")
                .build();
        client.start();
        return client;
    }
}