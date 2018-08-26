package com.wangsan.study.zookeeper.curator.study.discovery;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.concurrent.TimeUnit;

public class DaemonApp {

    public static void main(String[] args) throws Exception {
        String connectUrl = "localhost:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectUrl)
                .connectionTimeoutMs(1000)
                .sessionTimeoutMs(1000)
                .retryPolicy(retryPolicy)
                .namespace("wangsan/cluster2")
                .build();
        client.start();
        client.blockUntilConnected(5,TimeUnit.SECONDS);


        JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class);
        ServiceDiscovery serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                .client(client)
                .basePath("study")
                .serializer(serializer).build();
        serviceDiscovery.start();

        ServiceInstance thisInstance = ServiceInstance.<InstanceDetails>builder()
                .name("daemon")
                .payload(new InstanceDetails("aha"))
                .serviceType(ServiceType.DYNAMIC)
                .port((int) (65535 * Math.random())) // in a real application, you'd use a common port
                .build();


        serviceDiscovery.registerService(thisInstance);

        TimeUnit.SECONDS.sleep(100);
    }
}
