package com.wangsan.study.zookeeper.curator.study.cluster;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class DaemonAdder {

    public static void main(String[] args) throws Exception {
        Daemon.main(new String[]{"adder_1"});
        Daemon.main(new String[]{"adder_2"});
        System.in.read();
    }
}
