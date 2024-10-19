package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class RouteSMSPooler {
    public static JSONObject jsonRouteSMSProperty;
    private static Logger logger;

    public RouteSMSPooler() {
        // Load Configuration
        new Configuration();
//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        // Initate JSONRouteSMSProperty
        jsonRouteSMSProperty = new JSONObject();
        initiateJSONRouteSMSProperty();

        LoggingPooler.doLog(logger, "INFO", "RouteSMSPooler", "RouteSMSPooler", false, false, false, "",
                "Module RouteSMSPooler is initiated and ready to serve.", null);
    }

    public static void initiateJSONRouteSMSProperty() {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        // Query to Postgresql
        try {
            BasicDataSource bds = DataSource.getInstance().getBds(); // bds di sini tidak perlu diclose, karena akan close DataSource yang masih akan dipake oleh aplikasi pemanggil
            connection = bds.getConnection();
            statement = connection.createStatement();
            LoggingPooler.doLog(logger, "INFO", "RouteSMSPooler", "RouteSMSPooler", false, false, false, "",
                    "Database connection is load and initiated.", null);

            String query = "select routing_id, client_user_api, client_id, client_sender_id_id, telecom_id, vendor_id, vendor_sender_id_id, vendor_parameter_json, "
                    + "client_price_per_submit, client_price_per_delivery, vendor_price_per_submit, vendor_price_per_delivery, currency_id, fake_dr, voice_unit_second, voice_price_per_unit "
                    + "from routing_table_sms where is_active = true";

            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                JSONObject jsonDetail = new JSONObject();

                jsonDetail.put("routeId", resultSet.getString("routing_id"));
                jsonDetail.put("clientId", resultSet.getString("client_id"));
                jsonDetail.put("clientAPIUsername", resultSet.getString("client_user_api"));
                jsonDetail.put("clientSenderidId", resultSet.getString("client_sender_id_id"));
                jsonDetail.put("telecomId", resultSet.getString("telecom_id"));
                jsonDetail.put("vendorId", resultSet.getString("vendor_id"));
                jsonDetail.put("vendorSenderidId", resultSet.getString("vendor_sender_id_id"));
                jsonDetail.put("vendorParameter", resultSet.getString("vendor_parameter_json"));
                jsonDetail.put("clientPricePerSubmit", resultSet.getFloat("client_price_per_submit"));
                jsonDetail.put("clientPricePerDelivery", resultSet.getFloat("client_price_per_delivery"));
                jsonDetail.put("vendorPricePerSubmit", resultSet.getFloat("vendor_price_per_submit"));
                jsonDetail.put("vendorPricePerDelivery", resultSet.getFloat("vendor_price_per_delivery"));
                jsonDetail.put("fakeDr", resultSet.getBoolean("fake_dr"));
                jsonDetail.put("voice_unit_second", resultSet.getInt("voice_unit_second"));
                jsonDetail.put("voice_price_per_unit", resultSet.getDouble("voice_price_per_unit"));

                jsonRouteSMSProperty.put(resultSet.getString("routing_id").trim(), jsonDetail);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "RouteSMSPooler", "initiateJSONRouteSMSProperty", true, false, false, "",
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
                LoggingPooler.doLog(logger, "DEBUG", "RouteSMSPooler", "initiateJSONRouteSMSProperty", true, false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public static String getRoutedVendorId(String messageId, String clientSenderIdId, String telecomId) {
        LoggingPooler.doLog(logger, "DEBUG", "RouteSMSPooler", "getRoutedVendorId", false, false, true, "",
                " - clientSenderIdId: " + clientSenderIdId + "-telecomId:" + telecomId, null);
        String routedVendorId = "";

        // Check from jsonRouteSMSProperty
        String routeId = clientSenderIdId.trim() + "-" + telecomId.trim();
        if (jsonRouteSMSProperty.has(routeId)) {
            LoggingPooler.doLog(logger, "DEBUG", "RouteSMSPooler", "getRoutedVendorId", false, false, true, "",
                    " - clientSenderIdId: " + clientSenderIdId + "-telecomId:" + telecomId, null);
            JSONObject routing = jsonRouteSMSProperty.getJSONObject(routeId);
            routedVendorId = routing.getString("vendorId");
        }

        return routedVendorId;
    }

}
