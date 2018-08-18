package com.wangsan.study.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class CreateSession {

    public static void main(String[] args) throws Throwable {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);//刚开始重试间隔为1秒，之后重试间隔逐渐增加，最多重试不超过三次
        /*RetryPolicy retryPolicy1 = new RetryNTimes(3, 1000);//最大重试次数，和两次重试间隔时间
        RetryPolicy retryPolicy2 = new RetryUntilElapsed(5000, 1000);//会一直重试直到达到规定时间，第一个参数整个重试不能超过时间，第二个参数重试间隔
        //第一种方式
        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.0.3:2181", 5000,5000,retryPolicy);//最后一个参数重试策略
        */

        //第二种方式
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)//会话超时时间
                .connectionTimeoutMs(5000)//连接超时时间
                .retryPolicy(retryPolicy)
                .build();

        client.start();

        String path = client.create().creatingParentsIfNeeded()//若创建节点的父节点不存在会先创建父节点再创建子节点
                .withMode(CreateMode.EPHEMERAL)//withMode节点类型，
                .forPath("/curator/3", "131".getBytes());
        System.out.println(path);

        List<String> list = client.getChildren().forPath("/");
        System.out.println(list);


        //String re = new String(client.getData().forPath("/curator/3"));//只获取数据内容
        Stat stat = new Stat();
        String re = new String(client.getData().storingStatIn(stat)//在获取节点内容的同时把状态信息存入Stat对象
                .forPath("/curator/3"));
        System.out.println(re);
        System.out.println(stat);


        client.delete().guaranteed()//保障机制，若未删除成功，只要会话有效会在后台一直尝试删除
                .deletingChildrenIfNeeded()//若当前节点包含子节点
                .withVersion(-1)//指定版本号
                .forPath("/curator");


        NodeCache cache = new NodeCache(client, "/node_1");
        cache.start();
        cache.getListenable().addListener(new NodeCacheListener() {

            @Override
            public void nodeChanged() throws Exception {
                byte[] res = cache.getCurrentData().getData();
                System.out.println("data: " + new String(res));
            }
        });


        PathChildrenCache childrenCache = new PathChildrenCache(client, "/node_1", true);
        childrenCache.start();

        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework curator, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        System.out.println("add:" + event.getData());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("update:" + event.getData());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("remove:" + event.getData());
                        break;
                    default:
                        break;
                }
            }
        });

        client.create().creatingParentsIfNeeded()//若创建节点的父节点不存在会先创建父节点再创建子节点
                .withMode(CreateMode.EPHEMERAL)//withMode节点类型，
                .forPath("/node_1/1", "node_1_1".getBytes());

        new Thread(new Runnable() {
            @Override
            public void run() {
                CuratorFramework client2 = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
                        .sessionTimeoutMs(5000)//会话超时时间
                        .connectionTimeoutMs(5000)//连接超时时间
                        .retryPolicy(retryPolicy)
                        .build();
                client2.start();
                try {
                    client2.create().creatingParentsIfNeeded()//若创建节点的父节点不存在会先创建父节点再创建子节点
                            .withMode(CreateMode.EPHEMERAL)//withMode节点类型，
                            .forPath("/node_1/2", "node_1_2".getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();


        Thread.sleep(10000);

    }
}