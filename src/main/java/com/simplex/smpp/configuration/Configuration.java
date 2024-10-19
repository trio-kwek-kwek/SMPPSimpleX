package com.simplex.smpp.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Configuration {
    private static String pgHost;
    private static int pgPort;
    private static String pgDB;
    private static String pgUser;
    private static String pgPass;
    private static int pgConnPoolSize;

    private static String redisHost;
    private static int redisPort;
    private static String redisAuth;

//    private static String logConfigPath;

    private static String rabbitHost;
    private static String rabbitUser;
    private static String rabbitPass;
    private static String rabbitVirtualHost;

    public Configuration() {
        Properties prop = new Properties();
        InputStream input;

        try {
            input = Files.newInputStream(Paths.get("/nte/config/simplex.properties"));

            prop.load(input);

            pgHost = prop.getProperty("pg.host");
            pgPort = Integer.parseInt(prop.getProperty("pg.port"));
            pgDB = prop.getProperty("pg.name");
            pgUser = prop.getProperty("pg.username");
            pgPass = prop.getProperty("pg.password");
            pgConnPoolSize = Integer.parseInt(prop.getProperty("pg.connpoolsize"));

            redisHost = prop.getProperty("redis.host");
            redisPort = Integer.parseInt(prop.getProperty("redis.port"));
            redisAuth = prop.getProperty("redis.auth");

//            logConfigPath = prop.getProperty("log4j2.path");

            rabbitHost = prop.getProperty("rabbitmq.host");
            rabbitUser = prop.getProperty("rabbitmq.user");
            rabbitPass = prop.getProperty("rabbitmq.pass");
            rabbitVirtualHost = prop.getProperty("rabbitmq.virtualhost");

            input.close();
        } catch (IOException e) {
            System.out.println("Configuration module is not loaded. Please check the application.");
            System.exit(100);
        }
    }

    public static String getPgHost() {
        return pgHost;
    }

    public static int getPgPort() {
        return pgPort;
    }

    public static String getPgDB() {
        return pgDB;
    }

    public static String getPgUser() {
        return pgUser;
    }

    public static String getPgPass() {
        return pgPass;
    }

    public static int getPgConnPoolSize() {
        return pgConnPoolSize;
    }

    public static String getRedisHost() {
        return redisHost;
    }

    public static int getRedisPort() {
        return redisPort;
    }

    public static String getRedisAuth() {
        return redisAuth;
    }

//    public static String getLogConfigPath() {
//        return logConfigPath;
//    }

    public static String getRabbitHost() {
        return rabbitHost;
    }

    public static String getRabbitUser() {
        return rabbitUser;
    }

    public static String getRabbitPass() {
        return rabbitPass;
    }

    public static String getRabbitVirtualHost() {
        return rabbitVirtualHost;
    }


}
