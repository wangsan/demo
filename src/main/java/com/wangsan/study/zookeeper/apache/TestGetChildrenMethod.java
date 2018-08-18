package com.wangsan.study.zookeeper.apache;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 获得子节点列表的方法
 * Created by zhuzs on 2017/3/26.
 */
public class TestGetChildrenMethod implements Watcher {

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper;

    public static void main(String[] args) throws Exception {
        zooKeeper = new ZooKeeper("127.0.0.1:2181", 5000, new TestGetChildrenMethod());
        countDownLatch.await();

        // 创建父节点/test
        zooKeeper.create("/test", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // 在父节点/test下面创建a1节点
        zooKeeper.create("/test/a1", "456".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // 同步获得结果
        List<String> childrenList = zooKeeper.getChildren("/test", true);
        System.out.println("同步getChildren获得数据结果：" + childrenList);

        // 异步获得结果
        zooKeeper.getChildren("/test", true, new MyChildren2Callback(), null);

        // 在父节点/test下面创建a2节点
        zooKeeper.create("/test/a2", "456".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.create("/test/a3", "789".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Thread.sleep(10000);

    }

    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && null == event.getPath()) { // 连接时的监听事件
                countDownLatch.countDown();
            } else if (event.getType() == Event.EventType.NodeChildrenChanged) { // 子节点变更时的监听
                try {
                    System.out.println("重新获得Children，并注册监听：" + zooKeeper.getChildren(event.getPath(), true));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

class MyChildren2Callback implements AsyncCallback.Children2Callback {

    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        System.out.println("异步获得getChildren结果，rc=" + rc
                + "；path=" + path + "；ctx=" + ctx + "；children=" + children + "；stat=" + stat);
    }
}