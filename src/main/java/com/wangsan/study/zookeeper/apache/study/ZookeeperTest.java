package com.wangsan.study.zookeeper.apache.study;

import com.google.common.base.Charsets;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZookeeperTest {
    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 3000, new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("root event: " + event);
            }
        });

        System.out.println("start zookeeper");

        byte[] data = zooKeeper.getData("/", false, null);
        System.out.println("list is " + str(data));


        Stat exists = zooKeeper.exists("/testApache", new ExistWatcher(zooKeeper, "/testApache"));
        System.out.println("exists: " + exists);

        if (exists == null) {
            String create = zooKeeper.create("/testApache", bytes("create"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("create: " + create);
        }


        Stat setData = zooKeeper.setData("/testApache", bytes("setData"), -1);
        System.out.println("setData: " + setData);


        byte[] data2 = zooKeeper.getData("/testApache", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("---- /testApache getData watch event: " + event);
            }
        }, null);
        System.out.println("getData: " + str(data2));

        List<String> children = zooKeeper.getChildren("/", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("---- /testApache getChildren watch event: " + event);
            }
        });
        System.out.println("getChildren: ");
        children.forEach(System.out::println);


        Stat existsChild = zooKeeper.exists("/testApache/child", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("---- /testApache/child existsChild watch event: " + event);
            }
        });
        System.out.println("existsChild: " + exists);

        if (existsChild == null) {
            String create = zooKeeper.create("/testApache/child", bytes("child"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("create child: " + create);
        }


        zooKeeper.delete("/testApache/child", -1);
        System.out.println("delete child first: ");

        zooKeeper.delete("/testApache", -1);
        System.out.println("delete: ");


    }

    private static String str(byte[] data) throws UnsupportedEncodingException {
        return new String(data, Charsets.UTF_8.name());
    }

    private static byte[] bytes(String data) throws UnsupportedEncodingException {
        return data.getBytes(Charsets.UTF_8);
    }

    public static class ExistWatcher implements Watcher {
        private ZooKeeper zooKeeper;
        private String node;
        int i = 0;

        public ExistWatcher(ZooKeeper zooKeeper, String node) {
            this.zooKeeper = zooKeeper;
            this.node = node;
        }

        @Override
        public void process(WatchedEvent event) {
            System.out.println("---- /testApache exist watch event: " + event);
            System.out.println("--- i=" + i++);

            try {
                zooKeeper.exists(node, this);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
