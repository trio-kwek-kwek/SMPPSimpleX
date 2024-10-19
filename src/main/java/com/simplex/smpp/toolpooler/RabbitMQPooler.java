package com.simplex.smpp.toolpooler;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.simplex.smpp.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQPooler {
    private static Logger logger;
    private static ConnectionFactory factory;

    public RabbitMQPooler() {
        // Load logger configuration file
        new Configuration();

//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        logger = LogManager.getLogger("RABBITMQPOOLER");

        // Initiate logTool
        new LoggingPooler();

        // Initiate the RabbitMq Connection
        factory = new ConnectionFactory();
        factory.setHost(Configuration.getRabbitHost());
        factory.setPort(5672);
        factory.setUsername(Configuration.getRabbitUser());
        factory.setPassword(Configuration.getRabbitPass());
        factory.setVirtualHost(Configuration.getRabbitVirtualHost());

        // Update 2021-08-04 11:04:00
        factory.setRequestedHeartbeat(1);
        factory.setConnectionTimeout(5000);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);

        System.out.println("RabbitMQ Host: " + Configuration.getRabbitHost() + ", User: " + Configuration.getRabbitUser() + ", Password: " + Configuration.getRabbitPass() + ", VirtualHost: " + Configuration.getRabbitVirtualHost());
        LoggingPooler.doLog(logger, "DEBUG", "RabbitMQPooler", "RabbitMQPooler", false, false, false, "",
                "RabbitMQ Host: " + Configuration.getRabbitHost() + ", User: " + Configuration.getRabbitUser() + ", VirtualHost: " + Configuration.getRabbitVirtualHost(), null);
    }

    public Connection getConnection() {
        Connection connection = null;

        try {
            connection = factory.newConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "RabbitMQPooler", "getChannel", true, false, false, "",
                    "Failed to create Connection to RabbitMQ server. Error occured.", e);
        }

        return connection;
    }

    public Channel getChannel(Connection connection) {
        Channel channel = null;
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "RabbitMQPooler", "getChannel", true, false, false, "",
                    "Failed to create Channel. Error occured.", e);
        }

        return channel;
    }
}
