package com.wangsan.study.zookeeper.curator.study;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

public class CuratorTest {

    public static void main(String[] args) throws Exception {
        String zookeeperConnectionString = "localhost:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
        client.start();


        String curdPath = "/wangsan/curator";
        String path = client.create().creatingParentsIfNeeded().forPath(curdPath, "wangsan_persistence".getBytes());
        System.out.println("create path:" + path);

        Stat stat = new Stat();
        byte[] bytes = client.getData().storingStatIn(stat).usingWatcher(new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                System.out.println("get data watch " + event.getPath());
            }
        }).forPath(curdPath);
        System.out.println(new String(bytes));
        System.out.println(stat);

        Stat updateStat = client.setData().forPath(curdPath, "wangsan_update".getBytes());
        System.out.println("updateStat :" + updateStat);


        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(curdPath);
        System.out.println("delete");

        Stat existStat = client.checkExists().forPath(curdPath);
        System.out.println("checkExists :" + existStat);

    }

}
