package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TelecomPrefixPooler {
    public static JSONObject jsonPrefixProperty;
    private static Logger logger;

    public TelecomPrefixPooler() {
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
        jsonPrefixProperty = new JSONObject();
        initiateJSONPrefixProperty();

        LoggingPooler.doLog(logger, "INFO", "TelecomPrefixPooler", "TelecomPrefixPooler", false, false, false, "",
                "Module TelecomPrefixPooler is initiated and ready to serve.", null);
    }

    public static void initiateJSONPrefixProperty() {
        // Query to Postgresql
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            BasicDataSource bds = DataSource.getInstance().getBds(); // bds di sini tidak perlu diclose, karena akan close DataSource yang masih akan dipake oleh aplikasi pemanggil
            connection = bds.getConnection();
            statement = connection.createStatement();
            LoggingPooler.doLog(logger, "INFO", "TelecomPrefixPooler", "TelecomPrefixPooler", false, false, false, "",
                    "Database connection is load and initiated.", null);

            String query = "select country_code_and_prefix_id, country_code, prefix, telecom_id, country_code from country_code_prefix where is_active = true";

            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("id", resultSet.getString("country_code_and_prefix_id").trim());
                jsonDetail.put("countryCode", resultSet.getString("country_code"));
                jsonDetail.put("prefix", resultSet.getString("prefix"));
                jsonDetail.put("telecomId", resultSet.getString("telecom_id"));
                jsonDetail.put("countryCode", resultSet.getString("country_code"));

                jsonPrefixProperty.put(resultSet.getString("country_code_and_prefix_id").trim(), jsonDetail);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "TelecomPrefixPooler", "initiateJSONPrefixProperty", true, false, false, "",
                    "Failed to intiate JSONPrefixProperty. Error occured.", e);
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
                LoggingPooler.doLog(logger, "INFO", "TelecomPrefixPooler", "initiateJSONPrefixProperty", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }
}
