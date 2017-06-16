package com.umantis.poc.kafka.streams.gdpr.config;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import kafka.utils.ZKStringSerializer$;

/**
 * @author David Espinosa.
 */
@Configuration
public class ZookeeperConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperConfig.class);

    @Value("${zookeeper.server}")
    private String zookeeper;

    @Value("${zookeeper.session_timeout}")
    private int sessionTimeOut;

    @Value("${zookeeper.connection_timeout}")
    private int connectionTimeout;

    @Bean
    public ZkConnection getZookeeperConnection() {
        ZkConnection zkConnection = new ZkConnection(zookeeper, sessionTimeOut);
        return zkConnection;
    }

    @Bean
    public ZkClient getZookeeperClient() {
		LOGGER.info("Creating Zookeeper client");
        ZkClient zkClient = new ZkClient(getZookeeperConnection(), connectionTimeout, ZKStringSerializer$.MODULE$);
        return zkClient;
    }
}
