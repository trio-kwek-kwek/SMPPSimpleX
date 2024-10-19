package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;

public class DataSource {
    private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";

    private final BasicDataSource bds = new BasicDataSource();

    public DataSource() {

        new Configuration();// Initiate value

        String DB_URL = "jdbc:postgresql://" + Configuration.getPgHost() + ":" + Configuration.getPgPort() + "/" + Configuration.getPgDB();
        String DB_USER = Configuration.getPgUser().trim();
        String DB_PASSWORD = Configuration.getPgPass().trim();
        int CONN_POOL_SIZE = Configuration.getPgConnPoolSize();

        bds.setDriverClassName(DRIVER_CLASS_NAME);  //Set database driver name
        bds.setUrl(DB_URL); //Set database url
        bds.setUsername(DB_USER);   //Set database user
        bds.setPassword(DB_PASSWORD);   //Set database password
        bds.setInitialSize(CONN_POOL_SIZE); //Set the connection pool size

        bds.setMaxTotal(100);
        bds.setMaxOpenPreparedStatements(100);
        bds.setMinIdle(0);
        bds.setMaxIdle(10);
    }

    public static DataSource getInstance() {
        return DataSourceHolder.INSTANCE;
    }

    public BasicDataSource getBds() {
        return bds;
    }

    private static class DataSourceHolder {
        private static final DataSource INSTANCE = new DataSource();
    }
}
