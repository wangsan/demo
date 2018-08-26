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

public class Daemon {

    static String leaderPath = "/leader";
    static String servicePath = "/daemon";
    static String tempPath = "/temps";
    static String servicePathPrefix = servicePath + "/";
    static String tempPathPrefix = tempPath + "/";

    public static void main(String[] args) throws Exception {
        String nodeId = (args == null || args.length == 0) ? "daemon01" : args[0];


        String connectUrl = "localhost:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectUrl)
                .connectionTimeoutMs(1000)
                .sessionTimeoutMs(1000)
                .retryPolicy(retryPolicy)
                .namespace("wangsan/cluster")
                .build();
        client.start();

        client.create().orSetData().creatingParentsIfNeeded().forPath(leaderPath);
        client.create().orSetData().creatingParentsIfNeeded().forPath(servicePath);
        if (client.checkExists().forPath(tempPath) == null) {
            client.create().creatingParentsIfNeeded().forPath(tempPath);
        }


//        Stat stat = client.checkExists().forPath(servicePathPrefix + nodeId);
//        if (stat != null) {
//            System.err.println("same node id: " + nodeId);
//            return;
//        }
        String leaderPathNode = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(leaderPath + "/leaderPre_");
        System.out.println(leaderPathNode);
        System.out.println(ZKPaths.getNodeFromPath(leaderPathNode));

        List<String> stringList = client.getChildren().forPath(leaderPath);
        Collections.sort(stringList);
        boolean leader = ZKPaths.getNodeFromPath(leaderPathNode).equals(stringList.get(0));
        if (leader) {
            client.getChildren().usingWatcher(new MyCuratorWatcher(client)).forPath(tempPath);
        }

        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(tempPathPrefix + nodeId, nodeId.getBytes());

        if (leader) {
            IntStream.range(0, 10000).forEach(i -> {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    System.out.println(new Date());
                    client.getChildren().forPath(servicePath).forEach(System.out::println);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }


    }

    public static class MyCuratorWatcher implements CuratorWatcher {
        CuratorFramework client;

        public MyCuratorWatcher(CuratorFramework client) {
            this.client = client;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            System.out.println(event);
            client.getChildren().usingWatcher(this).forPath(tempPath);
            System.out.println("watch again");


            if (event.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)) {
                List<String> tempList = client.getChildren().forPath(tempPath);
                List<String> serviceList = client.getChildren().forPath(servicePath);
//                        client.transaction().forOperations();

                System.out.println("delete all");
                client.delete().guaranteed().deletingChildrenIfNeeded().forPath(servicePath);
                System.out.println("add all");
                for (String temp : tempList) {
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(servicePathPrefix + temp, temp.getBytes());
                }
            }
        }
    }

    ;
}
