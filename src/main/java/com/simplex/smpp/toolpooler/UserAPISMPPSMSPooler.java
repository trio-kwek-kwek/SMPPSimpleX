package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class UserAPISMPPSMSPooler {
    // Penampung SystemId and Password and IPAddress
    public static JSONObject jsonSysIdAccess;
    public static JSONObject jsonSimpleSysIdAccess;
    public static JSONObject jsonClientIdToAccess;
    public static JSONObject jsonSimpleClientIdToAccess;
    private static Logger logger;

    public UserAPISMPPSMSPooler() {
        // Load Configuration
        new Configuration();
//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        // Load jsonSysIdAccess
        loadJSONSMPPSysId();

        // Load jsonSimpleSysIdAddcess
        loadJSONSimpleSMPPSysId();

        LoggingPooler.doLog(logger, "INFO", "UserAPISMPPSMSPooler", "UserAPISMPPSMSPooler", false, false, false, "",
                "Module UserAPISMPPSMSPooler is initiated and ready to serve.", null);
    }

    public static void loadJSONSMPPSysId() {
        jsonSysIdAccess = new JSONObject();
        jsonClientIdToAccess = new JSONObject();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            BasicDataSource bds = DataSource.getInstance().getBds(); // bds di sini tidak perlu diclose, karena akan close DataSource yang masih akan dipake oleh aplikasi pemanggil
            connection = bds.getConnection();
            statement = connection.createStatement();

            String query = "select username, password, client_id, registered_ip_address from user_api where access_type = 'SMPPSMS' and is_active = true";

            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("sysId", resultSet.getString("username"));
                jsonDetail.put("password", resultSet.getString("password"));
                jsonDetail.put("ipAddress", resultSet.getString("registered_ip_address"));
                jsonDetail.put("clientId", resultSet.getString("client_id"));

                jsonSysIdAccess.put(resultSet.getString("username"), jsonDetail);

                jsonClientIdToAccess.put(resultSet.getString("client_id"), jsonDetail);
            }

            LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", false, false, false, "",
                    "jsonSysIdAccess and jsonClientIdToAccess are initiated and ready", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", true, false, false, "",
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
                LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public static void loadJSONSimpleSMPPSysId() {
        jsonSimpleSysIdAccess = new JSONObject();
        jsonSimpleClientIdToAccess = new JSONObject();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            BasicDataSource bds = DataSource.getInstance().getBds(); // bds di sini tidak perlu diclose, karena akan close DataSource yang masih akan dipake oleh aplikasi pemanggil
            connection = bds.getConnection();
            statement = connection.createStatement();

            String query = "select username, password, client_id, registered_ip_address from user_api where access_type = 'NEOSMPPSMS' and is_active = true";

            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("sysId", resultSet.getString("username"));
                jsonDetail.put("password", resultSet.getString("password"));
                jsonDetail.put("ipAddress", resultSet.getString("registered_ip_address"));
                jsonDetail.put("clientId", resultSet.getString("client_id"));
                System.out.println(resultSet.getString("username"));

                jsonSimpleSysIdAccess.put(resultSet.getString("username"), jsonDetail);

                jsonSimpleClientIdToAccess.put(resultSet.getString("client_id"), jsonDetail);
            }

            LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", false, false, false, "",
                    "jsonSimpleSysIdAccess and jsonSimpleClientIdToAccess are initiated and ready.", null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", true, false, false, "",
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
                LoggingPooler.doLog(logger, "DEBUG", "UserAPISMPPSMSPooler", "loadJSONSMPPSysId", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public String getSimpleClientId(String sysId) {
        String clientId = "";

        if (jsonSimpleSysIdAccess.has(sysId)) {
            JSONObject jsonData = jsonSimpleSysIdAccess.getJSONObject(sysId);

            clientId = jsonData.getString("clientId");
        }

        return clientId;
    }
}
