package com.wangsan.study.zookeeper.curator.discovery1;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.concurrent.TimeUnit;


// https://blog.csdn.net/mn960mn/article/details/51803128
public class AppServer {
    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 3));
        client.start();
        client.blockUntilConnected();

        /**
         * 指定服务的 地址，端口，名称
         */
        ServiceInstanceBuilder<ServiceDetail> sib = ServiceInstance.builder();
        sib.address("192.168.1.100");
        sib.port(8855);
        sib.name("tomcat");
        sib.payload(new ServiceDetail("主站web程序", 1));

        ServiceInstance<ServiceDetail> instance = sib.build();

        ServiceDiscovery<ServiceDetail> serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceDetail.class)
                .client(client)
                .serializer(new JsonInstanceSerializer<ServiceDetail>(ServiceDetail.class))
                .basePath(ServiceDetail.REGISTER_ROOT_PATH)
                .build();
        //服务注册
        serviceDiscovery.registerService(instance);
        serviceDiscovery.start();

        TimeUnit.SECONDS.sleep(70);

        serviceDiscovery.close();
        client.close();
    }
}
