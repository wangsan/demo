package com.wangsan.study.zookeeper.curator.study.cluster;

public class DaemonAdder2 {

    public static void main(String[] args) throws Exception {
        Daemon.main(new String[]{"adder_1"});
        Daemon.main(new String[]{"adder_3"});
        System.in.read();
    }
}
