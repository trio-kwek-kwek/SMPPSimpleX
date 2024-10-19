package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.postgresql.core.Utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SMSTransactionOperationPooler {
    private static Logger logger;

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private BasicDataSource bds = null;

    // CONNECTION, STATEMENT DLL AKAN DIINITIATE PER FUNCTION KARENA SUPAYA THREAD
    // SAFE
    public SMSTransactionOperationPooler() {
        // Load Configuration
        new Configuration();
//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("POOLER");

        // Initiate LoggingPooler
        new LoggingPooler();

        bds = DataSource.getInstance().getBds();

        LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "SMSTransactionOperationPooler", false,
                false, false, "",
                "Module SMSTransactionOperationPooler is initiated and ready to serve.", null);
    }

    // Function ini sama seperti diatas dipakai oleh SMPP server untuk save initial
    // data + multi mid untuk DR beberapa user/client
    public void saveInitialSMPPData(
            String messageId, LocalDateTime receiverDateTime, String batchId, String receiverData,
            String receiverClientResponse, String receiverclientIpAddress, LocalDateTime clientResponseDateTime,
            String msisdn, String message, String countryCode, String prefix, String telecomId, String trxStatus,
            String clientSenderId, String clientId, String apiUserName, double clientUnitPrice, String currency,
            String messageEncoding, int messageLength, int smsCount, String allMessageIds) {
        Connection connection = null;
        Statement statement = null;

        try {
            // For multi-concurrent-thread sake, all connection, resultset and statement
            // initiated per function
            connection = bds.getConnection();

            System.out.println(messageId + " SAVING INITIAL DATA");

            statement = connection.createStatement();

            System.out.println(messageId + " SAVING QUERY: " + statement.toString());

            String queryInsert = "INSERT INTO transaction_sms(message_id, transaction_date, msisdn, message, country_code, telecom_id, prefix, status_code, "
                    +
                    "receiver_type, application_id, client_id, currency, message_encodng, message_length, sms_count, client_price_per_unit, "
                    +
                    "client_price_total, client_sender_id, batch_id, api_username, multipart_messeage_ids) VALUES ('"
                    + messageId + "', '" + receiverDateTime.format(formatter) + "', '" + msisdn +
                    "', '" + quote(message) + "', '" + countryCode + "', '" + telecomId + "', '" + prefix + "', '"
                    + trxStatus + "', 'SMPP', 'SMPP_CORE', '" + clientId +
                    "', '" + currency + "', '" + messageEncoding + "', " + messageLength + ", " + smsCount + ", "
                    + String.format("%.5f", clientUnitPrice) +
                    ", " + String.format("%.5f", smsCount * clientUnitPrice) + ", '" + quote(clientSenderId) + "', '"
                    + batchId + "', '" + apiUserName + "', '" + allMessageIds + "')";

            System.out.println(messageId + " QUERY: " + queryInsert);

            statement.executeUpdate(queryInsert);

            // Insert statement_receiver
            String queryReceiver = "INSERT INTO transaction_sms_receiver(message_id, receiver_date_time, receiver_data, receiver_client_response, client_ip_address, "
                    +
                    "receiver_client_response_date_time) VALUES ('" + messageId + "', '"
                    + receiverDateTime.format(formatter) + "', '" + quote(receiverData) +
                    "', '" + quote(receiverClientResponse) + "', '" + quote(receiverclientIpAddress) + "', '"
                    + quote(clientResponseDateTime.format(formatter)) + "')";

            System.out.println(messageId + " QUERY RECEIVER: " + queryReceiver);

            statement.executeUpdate(queryReceiver);

        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "saveInitialSMPPData", true, false,
                    false, "",
                    "Failed to save initial data. Error occured.", e);
        } finally {
            try {
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "SMSTransactionOperationPooler", "saveInitialSMPPData", true,
                        false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public boolean getUserIsMultiMID(String userId) {
        boolean isMultiMID = false;

        Connection connection = null;
        Statement statement = null;
        // Statement statementReceiver = null;

        try {
            // For multi-concurrent-thread sake, all connection, resultset and statement
            // initiated per function
            connection = bds.getConnection();

            System.out.println(userId + " GETTING INITIAL DATA");

            statement = connection.createStatement();

            String query = "select is_multi_mid from user_api where username like '%" + userId + "%'";
            ResultSet rs = statement.executeQuery(query);
            System.out.println(userId + " QUERY: " + query);

            while (rs.next()) {
                isMultiMID = rs.getBoolean("is_multi_mid");
            }

            LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "getUserIsMultiMID", false, false,
                    false, "",
                    "userId: " + userId + " -> isMultiMID: " + isMultiMID, null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "getUserIsMultiMID", true, false,
                    false, "",
                    "Failed to get trx status. Error occured.", e);
        } finally {
            try {
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "SMSTransactionOperationPooler", "getUserIsMultiMID", true, false,
                        false, "",
                        "Failed to close query statement.", e);
            }
        }

        return isMultiMID;
    }

    public JSONObject getTransactionDetail(String messageId) {
        JSONObject jsonTransaction = new JSONObject();

        Connection connection = null;
        Statement statement = null;
        // Statement statementReceiver = null;

        try {
            // For multi-concurrent-thread sake, all connection, resultset and statement
            // initiated per function
            connection = bds.getConnection();

            statement = connection.createStatement();

            String query = "select transaction_date, msisdn, message, country_code, telecom_id, prefix, status_code, client_id, currency,"
                    + "message_encodng, message_length, sms_count, client_sender_id, api_username, multipart_messeage_ids from transaction_sms where message_id = '"
                    + messageId + "'";

            ResultSet rs = statement.executeQuery(query);

            while (rs.next()) {
                LocalDateTime trxDateTime = rs.getTimestamp("transaction_date").toLocalDateTime();
                String msisdn = rs.getString("msisdn").trim();
                String message = rs.getString("message").trim();
                String countryCode = rs.getString("country_code").trim();
                String telecomId = rs.getString("telecom_id").trim();
                String prefix = rs.getString("prefix").trim();
                String statusCode = rs.getString("status_code").trim();
                String clientId = rs.getString("client_id").trim();
                String currency = rs.getString("currency").trim();
                String messageEncoding = rs.getString("message_encodng").trim();
                int messageLength = rs.getInt("message_length");
                int smsCount = rs.getInt("sms_count");
                String clientSenderId = rs.getString("client_sender_id").trim();
                String apiUserName = rs.getString("api_username").trim();
                String multipartMesseageIds = rs.getString("multipart_messeage_ids").trim();

                // Put into jsonTransaction
                jsonTransaction.put("transactionDateTime", trxDateTime);
                jsonTransaction.put("msisdn", msisdn);
                jsonTransaction.put("message", message);
                jsonTransaction.put("countryCode", countryCode);
                jsonTransaction.put("telecomId", telecomId);
                jsonTransaction.put("prefix", prefix);
                jsonTransaction.put("statusCode", statusCode);
                jsonTransaction.put("clientId", clientId);
                jsonTransaction.put("currency", currency);
                jsonTransaction.put("encoding", messageEncoding);
                jsonTransaction.put("length", messageLength);
                jsonTransaction.put("count", smsCount);
                jsonTransaction.put("clientSenderId", clientSenderId);
                jsonTransaction.put("apiUserName", apiUserName);
                jsonTransaction.put("multipart_messeage_ids", multipartMesseageIds);
            }

            LoggingPooler.doLog(logger, "DEBUG", "SMSTransactionOperationPooler", "getTransactionDetail", false, false,
                    false, "",
                    "messageId: " + messageId + " -> trxDetail: " + jsonTransaction, null);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "getTransactionStatus", true, false,
                    false, "",
                    "Failed to get trx status. Error occured.", e);
        } finally {
            try {
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "getTransactionStatus", true,
                        false, false, "",
                        "Failed to close query statement.", e);
            }
        }

        return jsonTransaction;
    }

    public String quote(String toQuote) {
        String result = "";

        try {
            // Remove null characters from the string
            if (toQuote != null) {
                toQuote = toQuote.replace("\0", "");
            }

            result = Utils.escapeLiteral(null, toQuote, true).toString();
            result = result.replace("'", "'");
        } catch (SQLException e) {
            LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "quote", true, false, false, "",
                    "Failed to quote Postgresql string. Error occured.", e);
        }

        return result;
    }

    public void saveTransactionDLR(
            String messageId, String clientId, LocalDateTime dlrDateTime, String dlrBody, String dlrStatus,
            String dlrPushTo) {
        Connection connection = null;
        Statement statement = null;
        // Statement statementReceiver = null;

        try {
            // For multi-concurrent-thread sake, all connection, resultset and statement
            // initiated per function
            connection = bds.getConnection();

            statement = connection.createStatement();

            String queryInsert = "INSERT INTO transaction_sms_dlr(message_id, client_id, dlr_date_time, dlr_body, dlr_status, "
                    +
                    "dlr_push_to) VALUES ('" + messageId + "', '" + clientId + "', '" +
                    dlrDateTime.format(formatter) + "', '" + quote(dlrBody) + "', '" + dlrStatus + "', '" + dlrPushTo
                    + "') " +
                    "ON CONFLICT (message_id) DO UPDATE SET " +
                    "client_id = EXCLUDED.client_id, " +
                    "dlr_date_time = EXCLUDED.dlr_date_time, " +
                    "dlr_body = EXCLUDED.dlr_body, " +
                    "dlr_status = EXCLUDED.dlr_status, " +
                    "dlr_push_to = EXCLUDED.dlr_push_to, " +
                    "dlr_client_push_response = EXCLUDED.dlr_client_push_response";

            System.out.println(messageId + " QUERY: " + queryInsert);

            statement.execute(queryInsert);
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "saveTransactionDLR", true, false,
                    false, "",
                    "Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
        } finally {
            try {
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "SMSTransactionOperationPooler", "saveTransactionDLR", true, false,
                        false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public void insertTransactionDLRClientResponse(String messageId, String dlrClientResponse) {
        Connection connection = null;
        Statement statement = null;
        // Statement statementReceiver = null;

        String queryUpdate = "insert into transaction_sms_dlr_client_resp(message_id, dlr_client_push_response) values ('"
                + messageId + "', '" + dlrClientResponse + "') " +
                "ON CONFLICT (message_id) DO UPDATE SET " +
                "dlr_client_push_response = EXCLUDED.dlr_client_push_response;";

        try {
            // For multi-concurrent-thread sake, all connection, resultset and statement
            // initiated per function
            connection = bds.getConnection();

            statement = connection.createStatement();

            System.out.println(messageId + " QUERY: " + queryUpdate);

            statement.executeUpdate(queryUpdate);

        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "updateTransactionDLR", true, false,
                    false, "",
                    "Failed to execute insertTransactionDLRClientResponse. Error occured.", e);
        } finally {
            try {
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "SMSTransactionOperationPooler", "updateTransactionDLR", true,
                        false, false, "",
                        "Failed to close query statement.", e);
            }
        }
    }

    public void saveSMPPBindAttempt(
            String bindId, LocalDateTime trxDateTime, String apiUserName, String remoteIpAddress,
            String attemptActivity, String attemptResponse) {
        Connection connection = null;
        Statement statement = null;
        // Statement statementReceiver = null;

        try {
            // For multi-concurrent-thread sake, all connection, resultset and statement
            // initiated per function
            connection = bds.getConnection();

            statement = connection.createStatement();

            String queryInsert = "INSERT INTO public.smpp_bind_attempt(bind_id, api_username, remote_ip_address, attempt_activity, attempt_response, trx_datetime) "
                    + "VALUES ('" + bindId + "', '" + apiUserName + "', '" + remoteIpAddress + "', '" + attemptActivity
                    + "', '" + attemptResponse + "', '" + trxDateTime.format(formatter) + "')";

            statement.execute(queryInsert);

        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "INFO", "SMSTransactionOperationPooler", "saveSMPPAttempt", true, false, false,
                    "",
                    "Failed to intiate jsonSenderIdSMSProperty. Error occured.", e);
        } finally {
            try {
                if (statement != null)
                    statement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "SMSTransactionOperationPooler", "saveSMPPAttempt", true, false,
                        false, "",
                        "Failed to close query statement.", e);
            }
        }
    }
}
