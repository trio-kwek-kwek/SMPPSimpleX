package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ClientPropertyPooler {
    private static Logger logger;
    private static JSONObject jsonClientProperty;

    private BasicDataSource bds = null;

    public ClientPropertyPooler() {
        // Load Configuration
        new Configuration();
//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        // Initiate connection to Postgresql
        try {
            bds = DataSource.getInstance().getBds();
            LoggingPooler.doLog(logger, "INFO", "ClientPropertyPooler", "ClientPropertyPooler", false, false, false, "",
                    "Database connection is load and initiated.", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "ClientPropertyPooler", "ClientPropertyPooler", true, false, false, "",
                    "Failed to load connection to database server. Error occured.", e);
        }

        // Initate JSONClientProperty
        jsonClientProperty = new JSONObject();

        initiateJSONClientProperty();

        LoggingPooler.doLog(logger, "INFO", "ClientPropertyPooler", "ClientPropertyPooler", false, false, false, "",
                "Module ClientPropertyPooler is initiated and ready to serve.", null);
    }

    public void initiateJSONClientProperty() {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            connection = bds.getConnection();

            statement = connection
                    .prepareStatement("select client_id, client_name, client_city, currency_id, business_model from client where is_active = true");
            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("clientName", resultSet.getString("client_name"));
                jsonDetail.put("clientCity", resultSet.getString("client_city"));
                jsonDetail.put("currencyId", resultSet.getString("currency_id"));
                jsonDetail.put("businessModel", resultSet.getString("business_model"));

                jsonClientProperty.put(resultSet.getString("client_id"), jsonDetail);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "ClientPropertyPooler", "initiateJSONClientProperty", true, false, false, "",
                    "Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (statement != null)
                    statement.close();
                if (connection != null)
                    statement.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "ClientPropertyPooler", "initiateJSONClientProperty", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public String getBusinessMode(String clientId) {
        String businessModel = "PREPAID";

        try {
            if (jsonClientProperty.has(clientId.trim())) {
                businessModel = jsonClientProperty.getJSONObject(clientId.trim()).getString("businessModel");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return businessModel;
    }

    public String getCurrencyId(String clientId) {
        String clientCurrency = "";

        try {
            if (jsonClientProperty.has(clientId.trim())) {
                clientCurrency = jsonClientProperty.getJSONObject(clientId.trim()).getString("currencyId");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return clientCurrency;
    }
}
