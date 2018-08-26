package com.wangsan.study.zookeeper.curator.study.discovery;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class ExampleApp {

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


        JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class);
        ServiceDiscovery serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                .client(client)
                .basePath("study")
                .serializer(serializer).build();
        serviceDiscovery.start();

        ServiceInstance<InstanceDetails> thisInstance = ServiceInstance.<InstanceDetails>builder()
                .name("example")
                .payload(new InstanceDetails("aha"))
                .serviceType(ServiceType.DYNAMIC)
                .port((int) (65535 * Math.random())) // in a real application, you'd use a common port
                .build();


        serviceDiscovery.registerService(thisInstance);

        for(int i=0;i<1000;i++){
            TimeUnit.SECONDS.sleep(3);
            Collection<ServiceInstance<InstanceDetails>> instances = serviceDiscovery.queryForInstances("daemon");
            instances.forEach(n-> System.out.println(n));


            serviceDiscovery.queryForNames().forEach(System.err::println);
        }


    }
}
