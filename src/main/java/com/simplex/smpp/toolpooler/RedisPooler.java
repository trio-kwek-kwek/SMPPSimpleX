package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisPooler {
    private static String redisAuth;
    private static String redisHost;
    private static int redisPort;
    private final Logger logger;
    public StatefulRedisConnection<String, String> redisConnection;

    public RedisPooler() {
        // Load logger configuration file
        new Configuration();

//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        logger = LogManager.getLogger("REDISPOOLER");

        // Initiate redis property
        redisAuth = Configuration.getRedisAuth();
        redisHost = Configuration.getRedisHost();
        redisPort = Configuration.getRedisPort();

        LoggingPooler.doLog(logger, "INFO", "RedisPooler", "RedisPooler", false, false, false, "",
                "REDISPOOLER is initiated", null);
    }

    public RedisCommands<String, String> redisInitiateConnection() {
        if (redisConnection != null && redisConnection.isOpen()) {
            // Do nothing, it is open and connected anyway
            RedisCommands<String, String> syncCommands = redisConnection.sync();

            LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "",
                    "redisConnection is ALREADY initiated. Do nothing to re-initiate.", null);

            return syncCommands;
        } else {
            // Initiate connection to server
            RedisClient redisClient = RedisClient.create("redis://" + redisAuth + "@" + redisHost + ":" + redisPort);
            redisConnection = redisClient.connect();
            RedisCommands<String, String> syncCommands = redisConnection.sync();

            LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "",
                    "redisConnection is NOT initiated yet. Initiating it!", null);

            return syncCommands;
        }
    }

    public void redisSetWithExpiry(RedisCommands<String, String> syncCommands, String key, String value, int secondsExpiry) {
        syncCommands.set(key, value);
        syncCommands.expire(key, secondsExpiry);

        LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "",
                "Successfully set redis - key: " + key + ", value: " + value + ", expiry: " + secondsExpiry, null);
    }

    public String redisGet(RedisCommands<String, String> syncCommands, String key) {
        String result;

        result = syncCommands.get(key);

        if (result == null) {
            result = "";
        }

        LoggingPooler.doLog(logger, "DEBUG", "RedisPooler", "RedisPooler", false, false, false, "",
                "Successfully read redis - key: " + key, null);
        return result;
    }
}
