package com.simplex.smpp.server;

import com.cloudhopper.commons.charset.Charset;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.DeliverSmResp;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.util.DeliveryReceipt;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import com.simplex.smpp.toolpooler.*;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.lang.ref.WeakReference;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

class SMPPProcessor implements Runnable {
    private static Logger logger;
    private final WeakReference<SmppSession> sessionRef;
    private final String messageId;
    private final String systemId;
    private final String remoteIpAddress;
    private final String allMessageIds;
    private final String clientSenderId;
    private final String msisdn;
    private final String shortMessage;
    private final byte dataCoding;
    private final String clientId;
    private final Address mtSourceAddress;
    private final Address mtDestinationAddress;
    // Session ID
    private final String sessionId;
    // SUPPORTING CLASSES
    private final ClientPropertyPooler clientPropertyPooler;
    private final ClientBalancePooler clientBalancePooler;
    private final SMSTransactionOperationPooler smsTransactionOperationPooler;
    private final RabbitMQPooler rabbitMqPooler;
    private final Connection rabbitMqConnection;
    private final RedisPooler redisPooler;
    private final RedisCommands<String, String> redisCommand;
    String SMSQueueName = "SMPP_INCOMING";
    private String messageEncoding = "GSM";

    public SMPPProcessor(
            String theMessageId, String theSystemId, String theRemoteIpAddress, String allMessageIds,
            String theClientSenderId, String theMsisdn, byte[] theShortMessage, byte theDataCoding, String theClientId,
            Address theMtSourceAddress, Address theMtDestinationAddress, ClientPropertyPooler theClientPropertyPooler,
            ClientBalancePooler theClientBalancePooler, SMSTransactionOperationPooler theSmsTransactionOperationPooler,
            RabbitMQPooler theRabbitMqPooler, Connection theRabbitMqConnection, RedisPooler theRedisPooler,
            WeakReference<SmppSession> theSessionRef, Logger theLogger) {
        this.messageId = theMessageId;
        System.out.println("INITIATE msgId " + this.messageId);
        this.systemId = theSystemId;
        this.remoteIpAddress = theRemoteIpAddress;
        this.allMessageIds = allMessageIds;
        this.clientSenderId = theClientSenderId;
        this.msisdn = theMsisdn;
        this.dataCoding = theDataCoding;
        this.clientId = theClientId;
        this.mtSourceAddress = theMtSourceAddress;
        this.mtDestinationAddress = theMtDestinationAddress;

        this.sessionRef = theSessionRef;
        this.sessionId = Objects.requireNonNull(theSessionRef.get()).getConfiguration().getName();

        this.clientPropertyPooler = theClientPropertyPooler;
        this.clientBalancePooler = theClientBalancePooler;
        this.smsTransactionOperationPooler = theSmsTransactionOperationPooler;
        this.rabbitMqPooler = theRabbitMqPooler;
        this.rabbitMqConnection = theRabbitMqConnection;
        this.redisPooler = theRedisPooler;
        redisCommand = redisPooler.redisInitiateConnection();

        logger = theLogger;

        Charset theCharset = getCharsetByDataCoding(dataCoding);
        // Get the shortMessage
        if (theCharset == CharsetUtil.CHARSET_GSM) {
            messageEncoding = "GSM";
        } else if (theCharset == CharsetUtil.CHARSET_GSM7) {
            messageEncoding = "GSM";
        } else if (theCharset == CharsetUtil.CHARSET_UTF_8) {
            messageEncoding = "UCS2";
        } else {
            messageEncoding = "UCS2";
        }
        this.shortMessage = CharsetUtil.decode(theShortMessage, theCharset);

        LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "SMPPProcessor",
                false, false, false, messageId, "Session ID: " + sessionId + ". Incoming trx - messageId: " +
                        this.messageId + ", clientSenderId: " + this.clientSenderId + ", msisdn: " + this.msisdn +
                        ", dataCoding" + this.dataCoding + ", bShortMessage: " + Arrays.toString(theShortMessage) +
                        ", charSet: " + theCharset + ", shortmessage: " + this.shortMessage + ", clientId: " +
                        this.clientId,
                null);
    }

    private Charset getCharsetByDataCoding(byte dataCoding) {
        Charset theCharSet = CharsetUtil.CHARSET_GSM; // DEFAULT is GSM7

        try {
            switch (dataCoding) {
                case (byte) 0x00:
                    break;
                case (byte) 0x04:
                case (byte) 0x06:
                case (byte) 0x07:
                case (byte) 0x08:
                case (byte) 0x0D:
                    theCharSet = CharsetUtil.CHARSET_UCS_2;
                    break;
                default:
                    break;
            }
            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "getCharsetByDataCoding", false,
                    false, false, messageId, "sessionId: " + sessionId + " - dataCoding: " + dataCoding +
                            " -> charset: " + theCharSet,
                    null);
        } catch (Exception e) {
            e.printStackTrace();

            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "getCharsetByDataCoding", true,
                    false, false, messageId, "sessionId: " + sessionId + " - Failed to get charset of datacoding: " +
                            dataCoding + ". Error occured.",
                    e);

        }

        return theCharSet;
    }

    private boolean isSenderIdValid(String clientId, String clientSenderId) {
        boolean isValid = false;

        String senderIdId = clientSenderId.trim() + "-" + clientId.trim();

        LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "isSenderIdValid", false,
                false, false, messageId, "senderIdId: " + senderIdId, null);
        if (SenderIdSMSPooler.jsonSenderIdSMSProperty.has(senderIdId)) {
            isValid = true;
        }

        return isValid;
    }

    private JSONObject isPrefixValid(String msisdn) {
        JSONObject jsonPrefix = new JSONObject();
        jsonPrefix.put("isValid", false);

        Iterator<String> keys = TelecomPrefixPooler.jsonPrefixProperty.keys();

        while (keys.hasNext()) {
            String key = keys.next();
            if (msisdn.startsWith(key)) {
                jsonPrefix.put("isValid", true);
                jsonPrefix.put("prefix", TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("prefix"));
                jsonPrefix.put("telecomId", TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("telecomId"));
                jsonPrefix.put("countryCode",
                        TelecomPrefixPooler.jsonPrefixProperty.getJSONObject(key).get("countryCode"));
            }
        }

        return jsonPrefix;
    }

    private boolean isRouteDefined(String clientId, String clientSenderId, String apiUsername, String telecomId) {
        boolean isDefined = false;

        String routeId = clientSenderId.trim() + "-" + clientId.trim() + "-" + apiUsername.trim() + "-" +
                telecomId.trim();

        if (RouteSMSPooler.jsonRouteSMSProperty.has(routeId)) {
            isDefined = true;
        }

        System.out.println("ROUTE ID " + routeId + " is DEFINED.");
        return isDefined;
    }

    private void saveInitialData(String messageId, LocalDateTime receiverDateTime, String batchId, String receiverData,
                                 String receiverClientResponse,
                                 String receiverclientIpAddress, LocalDateTime clientResponseDateTime, LocalDateTime trxDateTime,
                                 String msisdn, String message, String countryCode, String prefix,
                                 String telecomId, String trxStatus, String receiverType, String clientSenderIdId, String clientSenderId,
                                 String clientId, String apiUserName,
                                 double clientUnitPrice, String currency, String messageEncoding, int messageLength, int smsCount,
                                 String deliveryStatus) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

            LocalDateTime now = LocalDateTime.now();
            this.smsTransactionOperationPooler.saveInitialSMPPData(messageId, now, "SMPPAPI", receiverData,
                    receiverClientResponse, this.remoteIpAddress, now, msisdn.trim(), message.trim(), countryCode,
                    prefix, telecomId, trxStatus, clientSenderId, this.clientId, this.systemId, clientUnitPrice,
                    currency, messageEncoding, messageLength, smsCount, this.allMessageIds);

            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor - saveInitialData",
                    "saveInitialData - " + this.sessionId, false, false, false, messageId,
                    "Successfully save Initial Data to Database.", null);

            String[] multiMessageIds = this.allMessageIds.trim().split(",");
            if (multiMessageIds.length > 0) {
                for (String multiMessageId : multiMessageIds) {
                    // Submit to Redis as initial data with expiry 7 days
                    int expiry = 7 * 24 * 60 * 60;
                    JSONObject jsonRedis = new JSONObject();
                    jsonRedis.put("messageId", multiMessageId);
                    jsonRedis.put("receiverDateTime", receiverDateTime.format(formatter));
                    jsonRedis.put("transactionDateTime", trxDateTime.format(formatter));
                    jsonRedis.put("msisdn", msisdn.trim()); // Have to be trimmed
                    jsonRedis.put("message", this.smsTransactionOperationPooler.quote(message).trim()); // Have to be
                    // trimmed
                    jsonRedis.put("telecomId", telecomId);
                    jsonRedis.put("countryCode", countryCode);
                    jsonRedis.put("prefix", prefix);
                    jsonRedis.put("errorCode", trxStatus);
                    jsonRedis.put("apiUserName", this.systemId);
                    jsonRedis.put("clientSenderIdId", clientSenderIdId);
                    jsonRedis.put("clientSenderId", clientSenderId);
                    jsonRedis.put("clientId", this.clientId);
                    jsonRedis.put("apiUserName", this.systemId);
                    jsonRedis.put("clientIpAddress", this.remoteIpAddress);
                    jsonRedis.put("receiverType", receiverType); // SMPP and HTTP only
                    jsonRedis.put("sysSessionId", this.sessionId);
                    jsonRedis.put("encoding", messageEncoding);

                    String redisKey = "trxdata-" + multiMessageId.trim();
                    String redisVal = jsonRedis.toString();

                    redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
                    LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor - saveInitialData",
                            "saveInitialData - " + this.sessionId, false, false, false, multiMessageId,
                            "Successfully save Initial Data to Database and REDIS.", null);
                }
            } else {
                // Above this for multiDR if needed
                // Submit to Redis as initial data with expiry 7 days
                int expiry = 7 * 24 * 60 * 60;
                JSONObject jsonRedis = new JSONObject();
                jsonRedis.put("messageId", messageId);
                jsonRedis.put("receiverDateTime", receiverDateTime.format(formatter));
                jsonRedis.put("transactionDateTime", trxDateTime.format(formatter));
                jsonRedis.put("msisdn", msisdn.trim()); // Have to be trimmed
                jsonRedis.put("message", this.smsTransactionOperationPooler.quote(message).trim()); // Have to be
                // trimmed
                jsonRedis.put("telecomId", telecomId);
                jsonRedis.put("countryCode", countryCode);
                jsonRedis.put("prefix", prefix);
                jsonRedis.put("errorCode", trxStatus);
                jsonRedis.put("apiUserName", this.systemId);
                jsonRedis.put("clientSenderIdId", clientSenderIdId);
                jsonRedis.put("clientSenderId", clientSenderId);
                jsonRedis.put("clientId", this.clientId);
                jsonRedis.put("apiUserName", this.systemId);
                jsonRedis.put("clientIpAddress", this.remoteIpAddress);
                jsonRedis.put("receiverType", receiverType); // SMPP and HTTP only
                jsonRedis.put("sysSessionId", this.sessionId);
                jsonRedis.put("encoding", messageEncoding);

                String redisKey = "trxdata-" + messageId.trim();
                String redisVal = jsonRedis.toString();

                redisPooler.redisSetWithExpiry(redisCommand, redisKey, redisVal, expiry);
                LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor - saveInitialData",
                        "saveInitialData - " + this.sessionId, false, false, false, messageId,
                        "Successfully save Initial Data to Database and REDIS.", null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor - saveInitialData",
                    "saveInitialData - " + this.sessionId, true, false, false, messageId,
                    "Failed saving initial data. Error occured.", e);
        }
    }

    private int getMessageLength(String encoding, String message) {
        int length = 300;

        int count = 0;

        if (encoding.equals("GSM")) {
            for (int x = 0; x < message.length(); x++) {
                String theChar = message.substring(x, x + 1);

                switch (theChar) {
                    case "[":
                    case "\\":
                    case "/":
                    case "€":
                    case "}":
                    case "{":
                    case "~":
                    case "^":
                    case "]":
                        count = count + 2;
                        break;
                    default:
                        count = count + 1;
                        break;
                }
            }
        } else {
            // UCS2
            count = message.length();
        }

        if (count > 0) {
            length = count;
        }

        return length;
    }

    private int getSmsCount(String message, String encoding) {
        switch (encoding) {
            case "GSM":
                if (message.length() <= 160) {
                    return 1;
                } else {
                    return (int) Math.ceil((double) message.length() / (double) 153);
                }
            case "UCS2":
                if (message.length() <= 70) {
                    return 1;
                } else {
                    return (int) Math.ceil((double) message.length() / (double) 67);
                }
            case "WHATSAPP":
                return 1;
            default:
                return (int) Math.ceil((double) message.length() / (double) 67);
        }
    }

    @SuppressWarnings("rawtypes")
    private void sendRequestPdu(SmppSession session, DeliverSm deliver) {
        try {
            WindowFuture<Integer, PduRequest, PduResponse> future = session.sendRequestPdu(deliver, 10000, false);
            if (!future.await()) {
                LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "sendRequestPdu", false,
                        false, true, messageId, "Failed to receive deliver_sm_resp within specified time.", null);
            } else if (future.isSuccess()) {
                DeliverSmResp deliverSmResp = (DeliverSmResp) future.getResponse();

                LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "sendRequestPdu", false, false,
                        true, messageId, "deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" +
                                deliverSmResp.getResultMessage() + "]",
                        null);
            } else {
                LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "sendRequestPdu", false,
                        false, true, messageId, "Failed to properly receive deliver_sm_resp: " + future.getCause(),
                        null);
            }
        } catch (Exception e) {
            e.printStackTrace();

            LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "sendRequestPdu", true, false,
                    true, messageId, "Failed to send PDU to client. Error occurs.", e);
        }
    }

    private void sendDeliveryReceipt(SmppSession session, Address mtDestinationAddress, Address mtSourceAddress,
                                     byte[] shortMessage, byte dataCoding) {

        DeliverSm deliver = new DeliverSm();
        deliver.setEsmClass(SmppConstants.ESM_CLASS_MT_SMSC_DELIVERY_RECEIPT);
        deliver.setSourceAddress(mtDestinationAddress);
        deliver.setDestAddress(mtSourceAddress);
        deliver.setDataCoding(dataCoding);
        try {
            deliver.setShortMessage(shortMessage);
        } catch (Exception e) {
            e.printStackTrace();
        }
        sendRequestPdu(session, deliver);
    }

    private void doProcessTheSMS() {
        System.out.println("PROCESSING msgId " + this.messageId);

        String errorCode = "002"; // DEFAULT errCode
        String deliveryStatus = "ACCEPTED";
        byte deliveryState = SmppConstants.STATE_ACCEPTED;
        byte esmeErrCode = SmppConstants.STATUS_OK;

        String prefix = "";
        String telecomId = "";
        String countryCode = "";
        String clientSenderIdId = "";
        String clientCurrency = "";
        Double clientPricePerSubmit = 0.00;

        try {
            LocalDateTime incomingDateTime = LocalDateTime.now();

            // Validate clientSenderId
            if (isSenderIdValid(this.clientId, this.clientSenderId)) {
                // clientSenderId is valid
                clientSenderIdId = clientSenderId.trim() + "-" + this.clientId.trim();

                // Validate msisdn
                JSONObject jsonMsisdn = isPrefixValid(this.msisdn);

                if (jsonMsisdn.getBoolean("isValid")) {
                    // Prefix is VALID
                    // Set MSISDN property
                    prefix = jsonMsisdn.getString("prefix");
                    telecomId = jsonMsisdn.getString("telecomId");
                    countryCode = jsonMsisdn.getString("countryCode");

                    // Validate ROUTE
                    if (isRouteDefined(this.clientId, this.clientSenderId, this.systemId, telecomId)) {
                        // ROUTE IS DEFINED

                        // Validate balance
                        // Get client currecnt
                        clientCurrency = this.clientPropertyPooler.getCurrencyId(this.clientId.trim());

                        // Check business model, prepaid or postpaid from clientPropertyPooler
                        String businessModel = this.clientPropertyPooler.getBusinessMode(this.clientId).trim();
                        LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS",
                                false, false, true, messageId, "Session ID: " + sessionId + ". Business Mode: " +
                                        businessModel,
                                null);

                        // routeId = clientSenderIdId + "-" + telecomId
                        String routeId = clientSenderId + "-" + this.clientId + "-" + this.systemId + "-" + telecomId;
                        JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);

                        // Get client price from jsonRoute
                        clientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit");

                        boolean isBalanceEnough = false; // DEFAULT IS FALSE - TO NOT SENDING WHEN ERROR HAPPENS.
                        if (businessModel.equals("PREPAID")) {
                            // Real balance deduction is in ROUTER. NOT IN SMPP FRONT END.
                            double divisionBalance = clientBalancePooler.getClientBalance(clientId);

                            isBalanceEnough = divisionBalance > clientPricePerSubmit;

                            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor",
                                    "doProcessTheSMS", false, false, true, messageId, "Session ID: " + sessionId +
                                            ". Checking initial balance - divisionBalance: " + divisionBalance,
                                    null);
                        } else {
                            isBalanceEnough = true; // POSTPAID.
                        }

                        // If balance is enough (will always be enough for postpaid)
                        if (isBalanceEnough) {
                            // BALANCE IS ENOUGH
                            errorCode = "002";
                            deliveryStatus = "ACCEPTED";

                            LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "doProcessTheSMS",
                                    false, false, true, messageId, "Session ID: " + sessionId +
                                            ". BALANCE ENOUGH, MESSAGE IS ACCEPTED. errCode: " + errorCode +
                                            ", esmeErrCode: STATUS OK.",
                                    null);
                            // Save initial data and send to SMPP_INCOMING queue to be processed by router

                        } else {
                            // BALANCE IS NOT ENOUGH
                            errorCode = "122"; // BALANCE NOT ENOUGH
                            deliveryStatus = "REJECTED";
                            deliveryState = SmppConstants.STATE_REJECTED;
                            esmeErrCode = SmppConstants.STATUS_SUBMITFAIL;

                            LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "doProcessTheSMS",
                                    false, false, true, messageId, "Session ID: " + sessionId +
                                            ". BALANCE NOT ENOUGH. errCode: " + errorCode +
                                            ", esmeErrCode: SUBMITE FAILED.",
                                    null);
                        }
                    } else {
                        // ROUTE IS NOT DEFINED
                        errorCode = "900"; // ROUTE NOT DEFINED
                        deliveryStatus = "REJECTED";
                        deliveryState = SmppConstants.STATE_REJECTED;
                        esmeErrCode = SmppConstants.STATUS_SUBMITFAIL;

                        LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "doProcessTheSMS",
                                false, false, true, messageId, "Session ID: " + sessionId +
                                        ". ROUTE IS NOT DEFINED. errCode: " + errorCode +
                                        ", esmeErrCode: SUBMIT FAILED.",
                                null);
                    }
                } else {
                    // Prefix is VALID
                    // Set MSISDN property
                    prefix = "0";
                    telecomId = "99900";
                    countryCode = "999";

                    // Validate ROUTE
                    if (isRouteDefined(this.clientId, this.clientSenderId, this.systemId, telecomId)) {
                        // ROUTE IS DEFINED

                        // Validate balance
                        // Get client currecnt
                        clientCurrency = this.clientPropertyPooler.getCurrencyId(this.clientId.trim());

                        // Check business model, prepaid or postpaid from clientPropertyPooler
                        String businessModel = this.clientPropertyPooler.getBusinessMode(this.clientId).trim();
                        LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS",
                                false, false, true, messageId, "MULTI-COUNTRY CASE - Session ID: " + sessionId +
                                        ". Business Mode: " + businessModel,
                                null);

                        // routeId = clientSenderIdId + "-" + telecomId
                        String routeId = clientSenderId + "-" + this.clientId + "-" + this.systemId + "-" + telecomId;
                        JSONObject jsonRoute = RouteSMSPooler.jsonRouteSMSProperty.getJSONObject(routeId);

                        // Get client price from jsonRoute
                        clientPricePerSubmit = jsonRoute.getDouble("clientPricePerSubmit");

                        boolean isBalanceEnough = false; // DEFAULT IS FALSE - TO NOT SENDING WHEN ERROR HAPPENS.
                        if (businessModel.equals("PREPAID")) {
                            // Real balance deduction is in ROUTER. NOT IN SMPP FRONT END.
                            double divisionBalance = clientBalancePooler.getClientBalance(clientId);

                            isBalanceEnough = divisionBalance > clientPricePerSubmit;

                            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor",
                                    "doProcessTheSMS", false, false, true, messageId,
                                    "MULTI-COUNTRY CASE - Session ID: " + sessionId +
                                            ". Checking initial balance - divisionBalance: " + divisionBalance,
                                    null);
                        } else {
                            isBalanceEnough = true; // POSTPAID.
                        }

                        // If balance is enough (will always be enough for postpaid)
                        if (isBalanceEnough) {
                            // BALANCE IS ENOUGH
                            errorCode = "002";
                            deliveryStatus = "ACCEPTED";

                            LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "doProcessTheSMS",
                                    false, false, true, messageId, "MULTI-COUNTRY CASE - Session ID: " + sessionId +
                                            ". BALANCE ENOUGH, MESSAGE IS ACCEPTED. errCode: " + errorCode +
                                            ", esmeErrCode: STATUS OK.",
                                    null);
                            // Save initial data and send to SMPP_INCOMING queue to be processed by router

                        } else {
                            // BALANCE IS NOT ENOUGH
                            errorCode = "122"; // BALANCE NOT ENOUGH
                            deliveryStatus = "REJECTED";
                            deliveryState = SmppConstants.STATE_REJECTED;
                            esmeErrCode = SmppConstants.STATUS_SUBMITFAIL;

                            LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "doProcessTheSMS",
                                    false, false, true, messageId, "MULTI-COUNTRY CASE - Session ID: " + sessionId +
                                            ". BALANCE NOT ENOUGH. errCode: " + errorCode +
                                            ", esmeErrCode: SUBMITE FAILED.",
                                    null);
                        }
                    } else {
                        // Prefix is not valid
                        errorCode = "113"; // UNREGISTERED PREFIX
                        deliveryStatus = "REJECTED";
                        deliveryState = SmppConstants.STATE_REJECTED;
                        esmeErrCode = SmppConstants.STATUS_INVNUMDESTS; // INVALID NUMBER DESTINATION

                        LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "doProcessTheSMS",
                                false, false, true, messageId, "Session ID: " + sessionId +
                                        ". PREFIX IS NOT DEFINED. errCode: " + errorCode +
                                        ", esmeErrCode: INVALID NUMBER DESTINATION.",
                                null);
                    }
                }
            } else {
                // clientSenderId is NOT VALID
                errorCode = "121"; // INVALID SENDERID
                deliveryStatus = "REJECTED";
                deliveryState = SmppConstants.STATE_REJECTED;
                esmeErrCode = SmppConstants.STATUS_INVSRCADR;

                LoggingPooler.doLog(logger, "INFO", "SMPPProcessor", "doProcessTheSMS", false,
                        false, true, messageId, "Session ID: " + sessionId + ". SENDERID IS NOT DEFINED. errCode: " +
                                errorCode + ", esmeErrCode: INVALID SOURCE ADDRESS.",
                        null);
            }

            /* Check Vendor Whatsapp to change ENCODING sms count change to 1 */
            String[] vendorWhatsapp = {"ARAN20230314", "ARTP20230319", "ARTP20230126", "ARTP20230207",
                    "NATH20230316", "PAIA20220704", "PATP20220704", "PAXT20220704", "SHST20230214", "WAFE21062321",
                    "WATI20220701", "AR0220230329", "ARTP20210821", "ARTP20230711"};
            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS",
                    false, false, true, messageId,
                    this.systemId + " - routingId: " + clientSenderId + "-" +
                            clientId + "-" + this.systemId + "-" + telecomId,
                    null);

            String vendorId = RouteSMSPooler.getRoutedVendorId(messageId,
                    clientSenderId + "-" + clientId + "-" + this.systemId, telecomId.trim());
            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS",
                    false, false, true, messageId,
                    this.systemId + " - vendorId: " + vendorId, null);

            boolean found = Arrays.asList(vendorWhatsapp).contains(vendorId);
            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS",
                    false, false, true, messageId,
                    this.systemId + " - found: " + found, null);

            if (found) {
                messageEncoding = "WHATSAPP";
            }

            // Get messageLength & smsCount
            int messageLength = getMessageLength(messageEncoding, shortMessage);
            int smsCount = getSmsCount(shortMessage, messageEncoding);

            // Save initial data
            System.out.println("Save " + this.messageId);

            String receiverData = "SMPP: clientSenderId: " + this.clientSenderId + ", msisdn: " + this.msisdn +
                    ", message: " + this.shortMessage + ", dataCoding" + dataCoding;
            String receiverResponse = "errorCode: " + errorCode + ", deliveryStatus: " + deliveryState;
            LocalDateTime responseDateTime = LocalDateTime.now();
            String receiverType = "SMPP";
            saveInitialData(this.messageId, incomingDateTime, this.sessionId, receiverData,
                    receiverResponse, this.remoteIpAddress, responseDateTime, incomingDateTime, msisdn, shortMessage,
                    countryCode,
                    prefix, telecomId, errorCode, receiverType, clientSenderIdId, clientSenderId, clientId,
                    this.systemId,
                    clientPricePerSubmit, clientCurrency, messageEncoding, messageLength, smsCount, deliveryStatus);

            // Process sending DLR - ONLY Failed one. ACCEPTED one will get DR after
            // processed in ROUTER
            if (!errorCode.trim().startsWith("00")) {
                int submitCount = 0;
                DeliveryReceipt dlrReceipt = new DeliveryReceipt(messageId, submitCount, 0, new DateTime(),
                        new DateTime(), deliveryState, esmeErrCode, shortMessage);

                sendDeliveryReceipt(sessionRef.get(), this.mtDestinationAddress, this.mtSourceAddress,
                        dlrReceipt.toShortMessage().getBytes(), this.dataCoding);
                LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS - " +
                                this.sessionId, false, false, false, messageId,
                        "Sending DLR with session: " +
                                Objects.requireNonNull(sessionRef.get()).getConfiguration().getName() + ". DLR: " +
                                dlrReceipt.toShortMessage(),
                        null);
            } else {
                // SUBMIT TO QUEUE SMPP_INCOMING - Need specific channel per thread, do open new
                // channel and close it after
                // Submit to Queue for further process
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

                JSONObject jsonIncoming = new JSONObject();

                jsonIncoming.put("messageId", messageId);
                jsonIncoming.put("receiverDateTime", incomingDateTime.format(formatter));
                jsonIncoming.put("transactionDateTime", responseDateTime.format(formatter));
                jsonIncoming.put("msisdn", msisdn);
                jsonIncoming.put("message", this.smsTransactionOperationPooler.quote(shortMessage));
                jsonIncoming.put("telecomId", telecomId);
                jsonIncoming.put("countryCode", countryCode);
                jsonIncoming.put("prefix", prefix);
                jsonIncoming.put("errorCode", errorCode);
                jsonIncoming.put("apiUserName", this.systemId);
                jsonIncoming.put("clientSenderIdId", clientSenderIdId); // senderIdId-clientId
                jsonIncoming.put("clientSenderId", clientSenderId);
                jsonIncoming.put("clientId", this.clientId);
                jsonIncoming.put("apiUserName", this.systemId);
                jsonIncoming.put("clientIpAddress", this.remoteIpAddress);
                jsonIncoming.put("receiverType", receiverType); // SMPP and HTTP only
                jsonIncoming.put("smsChannel", receiverType);
                jsonIncoming.put("sysSessionId", this.sessionId);
                jsonIncoming.put("messageLength", messageLength);
                jsonIncoming.put("messageCount", smsCount);
                jsonIncoming.put("clientPricePerSubmit", clientPricePerSubmit);
                jsonIncoming.put("encoding", messageEncoding);

                LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS - " +
                                this.sessionId, false, false, false, messageId,
                        "jsonIncoming: " + jsonIncoming, null);

                Channel channel = rabbitMqPooler.getChannel(rabbitMqConnection);

                channel.queueDeclare(SMSQueueName, true, false, false, null);
                channel.basicPublish("", SMSQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        jsonIncoming.toString().getBytes());
                LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS - " +
                                this.sessionId, false, false, false, messageId,
                        "jsonIncoming: " + jsonIncoming +
                                " published SUCCESSfully!",
                        null);

                channel.close();
            }
        } catch (Exception e) {
            e.printStackTrace();

            LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "doProcessTheSMS - " +
                            this.sessionId, true, false, false, messageId,
                    "Failed to process incoming sms message. Error occured.", e);
        }
    }

    @Override
    public void run() {
        LoggingPooler.doLog(logger, "DEBUG", "SMPPProcessor", "RUN", false, false, false,
                messageId, "Session ID: " + sessionId + ". Processing the SMS.", null);

        doProcessTheSMS();
    }

}
