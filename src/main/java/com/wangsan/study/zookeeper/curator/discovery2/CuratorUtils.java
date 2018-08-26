package com.wangsan.study.zookeeper.curator.discovery2;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorUtils {

    public static CuratorFramework getCuratorClient() throws Exception {
        return CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 3));
    }
}