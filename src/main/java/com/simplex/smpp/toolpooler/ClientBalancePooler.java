package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ClientBalancePooler {
    private static Logger logger;

    private BasicDataSource bds = null;

    public ClientBalancePooler() {
        new Configuration();    // Load Configuration
//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        logger = LogManager.getLogger("POOLER");    // Setup logger

        new LoggingPooler();    // Initiate LoggingPooler

        try {
            bds = DataSource.getInstance().getBds();    // Initiate connection to Postgresql
            LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "ClientBalancePooler", false, false, false, "",
                    "Database connection is load and initiated.", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "ClientBalancePooler", true, false, false, "",
                    "Failed to load connection to database server. Error occured.", e);
        }

        LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "ClientBalancePooler", false, false, false, "",
                "Module ClientBalancePooler is initiated and ready to serve.", null);
    }

    public double getClientBalance(String clientId) {
        double balance = 0.00;
        Connection getBalConnection = null;
        Statement getBalStatement = null;
        ResultSet resultSet = null;

        try {
            getBalConnection = bds.getConnection();
            String query = "select now_balance from client_balance where client_id = '" + clientId.trim() + "'";
            getBalStatement = getBalConnection.createStatement();

            resultSet = getBalStatement.executeQuery(query);

            if (!resultSet.next()) {
                balance = 0.00000;    // Record is not found - no balance
            } else {
                balance = resultSet.getDouble("now_balance");    // Record is found - use first one
                LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "getClientBalance", false, false, false, "",
                        "clientId: " + clientId + " -> balance: " + String.format("%.5f", balance), null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "ClientBalancePooler", "getClientBalance", true, false, false, "",
                    "Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (getBalStatement != null)
                    getBalStatement.close();
                if (getBalConnection != null)
                    getBalConnection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "ClientBalancePooler", "getClientBalance", true, false, false,
                        "", "Failed to close query statement.", e);
            }
        }

        return balance;
    }
}
