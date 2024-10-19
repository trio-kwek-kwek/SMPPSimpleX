package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class SenderIdSMSPooler {
    public static JSONObject jsonSenderIdSMSProperty;
    private static Logger logger;

    public SenderIdSMSPooler() {
        // Load Configuration
        new Configuration();
//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        // Initate JSONClientProperty
        jsonSenderIdSMSProperty = new JSONObject();
        initiateJsonSenderIdSMSProperty();

        LoggingPooler.doLog(logger, "INFO", "SMSSenderIdPooler", "SMSSenderIdPooler", false, false, false, "",
                "Module SMSSenderIdPooler is initiated and ready to serve.", null);
    }

    public static void initiateJsonSenderIdSMSProperty() {
        // Query to Postgresql
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            BasicDataSource bds = DataSource.getInstance().getBds(); // bds di sini tidak perlu diclose, karena akan close DataSource yang masih akan dipake oleh aplikasi pemanggil
            connection = bds.getConnection();
            statement = connection.createStatement();
            LoggingPooler.doLog(logger, "INFO", "SMSSenderIdPooler", "SMSSenderIdPooler", false, false, false, "",
                    "Database connection is load and initiated.", null);

            String query = "select client_sender_id_id, sender_id, client_id, masking from client_senderid_sms where is_active = true";

            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("id", resultSet.getString("client_sender_id_id").trim());
                jsonDetail.put("senderId", resultSet.getString("sender_id"));
                jsonDetail.put("clientId", resultSet.getString("client_id"));
                jsonDetail.put("masking", resultSet.getString("masking"));

                jsonSenderIdSMSProperty.put(resultSet.getString("client_sender_id_id").trim(), jsonDetail);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMSSenderIdPooler", "initiateJsonSenderIdSMSProperty", true, false, false, "",
                    "Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
        } finally {
            try {
                if (resultSet != null)
                    resultSet.close();
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "SMSSenderIdPooler", "initiateJsonSenderIdSMSProperty", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }
}
