package com.simplex.smpp.server;

import com.cloudhopper.commons.charset.Charset;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.commons.gsm.GsmUtil;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.util.SmppUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.Connection;
import com.simplex.smpp.toolpooler.*;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.lang.ref.WeakReference;
import java.lang.reflect.Type;
import java.util.*;

public class SMPPSessionHandler extends DefaultSmppSessionHandler {
    private static Logger logger;
    private final WeakReference<SmppSession> sessionRef;
    private final String systemId;
    private final String remoteIpAddress;
    private final String clientId;
    private final String sessionId;

    // Supporting class
    private final Tool tool;
    private final SMPPEnquiryLinkPooler smppEnquiryLinkPooler;
    private final ClientPropertyPooler clientPropertyPooler;
    private final ClientBalancePooler clientBalancePooler;
    private final SMSTransactionOperationPooler smsTransactionOperationPooler;
    private final RabbitMQPooler rabbitMqPooler;
    private final Connection rabbitMqConnection;
    private final RedisPooler redisPooler;
    private final RedisCommands<String, String> redisCommand;
    private final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();

    public SMPPSessionHandler(
            SmppSession session, String theSessionId, String theRemoteIpAddress,
            String theClientId, Tool theTool, SMPPEnquiryLinkPooler theSmppEnquiryLinkPooler,
            ClientPropertyPooler theClientPropertyPooler, ClientBalancePooler theClientBalancePooler,
            SMSTransactionOperationPooler theSmsTransactionOperationPooler, RabbitMQPooler theRabbitMqPooler,
            Connection theRabbitMqConnection, RedisPooler theRedisPooler,
            Logger theLogger) {
        this.sessionRef = new WeakReference<SmppSession>(session);
        logger = theLogger;

        this.systemId = session.getConfiguration().getSystemId();
        this.remoteIpAddress = theRemoteIpAddress;
        this.clientId = theClientId;
        this.sessionId = theSessionId;

        this.tool = theTool;
        this.smppEnquiryLinkPooler = theSmppEnquiryLinkPooler;
        this.clientPropertyPooler = theClientPropertyPooler;
        this.clientBalancePooler = theClientBalancePooler;
        this.smsTransactionOperationPooler = theSmsTransactionOperationPooler;
        this.rabbitMqPooler = theRabbitMqPooler;
        this.rabbitMqConnection = theRabbitMqConnection;
        this.redisPooler = theRedisPooler;
        this.redisCommand = redisPooler.redisInitiateConnection();
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

            LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler",
                    "getCharsetByDataCoding - " + this.sessionId, false, false, false, "",
                    "dataCoding: " + dataCoding + " -> charset: " + theCharSet, null);
        } catch (Exception e) {
            e.printStackTrace();

            LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler",
                    "getCharsetByDataCoding - " + this.sessionId, true, false, false, "",
                    "Failed to get charset of datacoding: " + dataCoding + ". Error occurs.", e);

        }

        return theCharSet;
    }

    private HashMap<String, String> combineMessage(String origin, String destination, String messageId, int byteId,
                                                   int totalMessageCount, int currentCount, byte[] shortMessagePart) {
        byte[] combinedMessage = new byte[]{};
        HashMap<String, String> result = new HashMap<String, String>();
        StringBuilder messageAllIds = new StringBuilder();
        String tempMessageId = messageId;

        if (currentCount > 1) {
            String redisKeyIn = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-" + totalMessageCount
                    + "-" + (currentCount - 1);

            String jsonRedisValCombine = redisPooler.redisGet(redisCommand, redisKeyIn);
            System.out.println("getting first messageID redisKeyIn: " + redisKeyIn + ". jsonRedisValCombine: "
                    + jsonRedisValCombine);
            Type type = new TypeToken<HashMap<String, String>>() {
            }.getType();
            HashMap<String, String> mapRedisValCombine = gson.fromJson(jsonRedisValCombine, type);
            messageId = mapRedisValCombine.get("first_message_id");
            System.out.println("getting first messageID messageId: " + messageId + ".");
        }

        int expirySeconds = 24 * 60 * 60;

        // Convert byte[] to string using base64 to make sure the consistency
        String encodedByte = Base64.getEncoder().encodeToString(shortMessagePart);

        // Write to redis
        String redisKey = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-" + totalMessageCount + "-"
                + currentCount;

        HashMap<String, String> mapRedisVal = new HashMap<String, String>();
        mapRedisVal.put("encoded_byte", encodedByte);
        mapRedisVal.put("first_message_id", messageId);
        mapRedisVal.put("temp_message_id", tempMessageId);
        System.out.println("part: " + currentCount + ", temp_message_id: " + tempMessageId);

        String jsonRedisVal = gson.toJson(mapRedisVal);

        redisPooler.redisSetWithExpiry(redisCommand, redisKey, jsonRedisVal, expirySeconds);

        // Check if all messages are already in redis
        boolean isComplete = (totalMessageCount == currentCount);

        System.out.println("isComplete: " + isComplete);

        // If complete, join the message
        System.out.println("Checking final ISCOMPLETE: " + isComplete);
        if (isComplete) {
            for (int x = 1; x <= totalMessageCount; x++) {
                String redisKeyIn = "multipartsms-" + origin + "-" + destination + "-" + byteId + "-"
                        + totalMessageCount + "-" + x;

                String jsonRedisValCombine = redisPooler.redisGet(redisCommand, redisKeyIn);
                System.out.println(x + ". redisKeyIn: " + redisKeyIn + ". jsonRedisValCombine: " + jsonRedisValCombine);
                Type type = new TypeToken<HashMap<String, String>>() {
                }.getType();
                HashMap<String, String> mapRedisValCombine = gson.fromJson(jsonRedisValCombine, type);
                String encodedByteRedis = mapRedisValCombine.get("encoded_byte");
                byte[] redisValByte = Base64.getDecoder().decode(encodedByteRedis);
                System.out.println(x + ". redisValByte: " + Arrays.toString(redisValByte));

                System.out.println(x + ". combinedMessage: " + Arrays.toString(combinedMessage) + ", redisValByte: "
                        + Arrays.toString(redisValByte));
                System.out.println(x + ". combinedMessage length: " + combinedMessage.length + ", redisValByte length: "
                        + redisValByte.length);
                byte[] xByte = new byte[combinedMessage.length + redisValByte.length];
                System.out.println(x + ". xByte: " + Arrays.toString(xByte));

                System.arraycopy(combinedMessage, 0, xByte, 0, combinedMessage.length);
                System.arraycopy(redisValByte, 0, xByte, combinedMessage.length, redisValByte.length);
                System.out.println("new xByte: " + Arrays.toString(xByte));
                if (x > 1) {
                    messageAllIds.append(", ").append(mapRedisValCombine.get("temp_message_id"));
                    System.out.println("comnbine part: " + x + ", messageAllIds: " + messageAllIds);
                } else {
                    messageAllIds = new StringBuilder(mapRedisValCombine.get("temp_message_id"));
                    System.out.println("comnbine part: " + x + ", messageAllIds: " + messageAllIds);
                }

                combinedMessage = xByte;
            }
            encodedByte = Base64.getEncoder().encodeToString(combinedMessage);
        }
        result.put("encoded_byte", encodedByte);
        result.put("first_message_id", messageId);
        result.put("temp_message_id", tempMessageId);
        result.put("message_all_ids", String.valueOf(messageAllIds));

        return result;
    }

    public PduResponse firePduRequestReceived(PduRequest pduRequest) {
        SmppSession session = sessionRef.get();

        LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived",
                false, false, true, "", this.sessionId + " - pduRequest: " + pduRequest.toString(), null);
        LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived",
                false, false, true, "", this.sessionId + " - json pduRequest: " + gson.toJson(pduRequest), null);

        // Default value of response
        PduResponse response = pduRequest.createResponse();

        if (pduRequest.getCommandId() == SmppConstants.CMD_ID_ENQUIRE_LINK) {
            EnquireLink enqResp = new EnquireLink();
            int seqNumber = pduRequest.getSequenceNumber();
            enqResp.setSequenceNumber(seqNumber);
            response = enqResp.createResponse();

            System.out.println("Enquire Link Request from "
                    + Objects.requireNonNull(session).getConfiguration().getSystemId() + " - " +
                    this.clientId + " - " + pduRequest + " - sequenceNumber: " + seqNumber);
            this.smppEnquiryLinkPooler.logEnquiryLink(clientId, this.sessionId, "ENQUIRE_LINK");
        } else if (pduRequest.getCommandId() == SmppConstants.CMD_ID_BIND_TRANSCEIVER) {
            BindTransceiverResp resp = new BindTransceiverResp();
            resp.setSequenceNumber(pduRequest.getSequenceNumber());
            response = resp;

            LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived", false, false,
                    true, "",
                    this.sessionId + " - " + Objects.requireNonNull(session).getConfiguration().getSystemId()
                            + " - PDU Request: CMD_ID_BIND_TRANSCEIVER. Responded!",
                    null);

        } else if (pduRequest.getCommandId() == SmppConstants.CMD_ID_SUBMIT_SM) {
            try {
                LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived", false, false,
                        true, "",
                        this.sessionId + " - " + Objects.requireNonNull(session).getConfiguration().getSystemId()
                                + " - PDU Request: CMD_ID_SUBMIT_SMS.",
                        null);

                LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived",
                        false, false, true, "", this.sessionId + " - json pduRequest: " + gson.toJson(pduRequest),
                        null);

                this.smppEnquiryLinkPooler.logEnquiryLink(clientId, this.sessionId, "SUBMIT_SM");

                // Create messageId
                String messageId = tool.generateUniqueID();
                LoggingPooler.doLog(logger, "INFO", "SimpleSMPPSessionHandler", "firePduRequestReceived", false, false,
                        true, "",
                        this.sessionId + " - " + session.getConfiguration().getSystemId() + " - Assigning messageId: "
                                + messageId,
                        null);

                // Check if message with UDH
                SubmitSm mt = (SubmitSm) pduRequest;

                boolean isUdh = SmppUtil.isUserDataHeaderIndicatorEnabled(mt.getEsmClass());
                LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived", false, false,
                        true, "",
                        this.sessionId + " - " + session.getConfiguration().getSystemId() + " - isUDH (multipart sms): "
                                + isUdh,
                        null);

                Address mtSourceAddress = mt.getSourceAddress();
                String clientSenderId = mtSourceAddress.getAddress().trim();
                Address mtDestinationAddress = mt.getDestAddress();
                String msisdn = mtDestinationAddress.getAddress().trim();

                if (isUdh) {
                    // Handle multipart sms
                    byte[] userDataHeader = GsmUtil.getShortMessageUserDataHeader(mt.getShortMessage());
                    // lets assume the latest value is current Index SMPP Multipart
                    // second after latest value is max Index SMPP Multipart
                    int[] headerEncode = new int[userDataHeader.length];
                    int j = 0;
                    for (int i = (userDataHeader.length - 1); i >= 0; i--) {
                        LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived",
                                false, false, true, "",
                                this.sessionId + " - " + session.getConfiguration().getSystemId() +
                                        " - index: " + i +
                                        " - real value: " + (userDataHeader[i]) +
                                        " - value encode: " + (userDataHeader[i] & 0xff),
                                null);
                        headerEncode[j] = userDataHeader[i] & 0xff;
                        j++;
                    }

                    int byteId = headerEncode[2] & 0xff;
                    int maxMessageCount = headerEncode[1] & 0xff;
                    int currentMessageCount = headerEncode[0] & 0xff;
                    LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived", false,
                            false, true, "",
                            this.sessionId + " - " + session.getConfiguration().getSystemId() +
                                    " - byteId: " + byteId + " - maxMessageCount: " + maxMessageCount
                                    + " - currentMessageCount: " + currentMessageCount,
                            null);

                    byte dataCoding = mt.getDataCoding();
                    byte[] shortMessage = GsmUtil.getShortMessageUserData(mt.getShortMessage());

                    Charset theCharset = getCharsetByDataCoding(dataCoding);

                    String theSMSPart = CharsetUtil.decode(shortMessage, theCharset);
                    LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived", false,
                            false, true, "",
                            this.sessionId + " - " + session.getConfiguration().getSystemId() + " - theSMSPart: "
                                    + theSMSPart + " - theSMSPart length: " + theSMSPart.length(),
                            null);

                    // Combine all shortMessage
                    HashMap<String, String> result = combineMessage(clientSenderId, msisdn, messageId, byteId,
                            maxMessageCount, currentMessageCount, shortMessage);
                    byte[] allSMSMessage = Base64.getDecoder().decode(result.get("encoded_byte"));
                    messageId = result.get("first_message_id");
                    String tempMessageId = result.get("temp_message_id");
                    String allMessageIds = result.get("message_all_ids");

                    if (maxMessageCount == currentMessageCount && allSMSMessage.length != 0) {
                        // NULL, no complete yet the combination process
                        // Do not process anything
                        // Complete the combination process
                        // Run thread smppIncomingTrxProcessor
                        LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived",
                                false, false, true, messageId,
                                "Incoming sms is with UDH. The byteId: " + byteId + ", maxMessageCount: "
                                        + maxMessageCount + ", currentMessageCount: " +
                                        currentMessageCount + ", thisSMS: " + theSMSPart,
                                null);

                        if (!this.smsTransactionOperationPooler
                                .getUserIsMultiMID(session.getConfiguration().getSystemId())) {
                            allMessageIds = "";
                        }

                        Thread incomingTrxProcessor = new Thread(new SMPPProcessor(messageId, systemId, remoteIpAddress,
                                allMessageIds, clientSenderId, msisdn, allSMSMessage,
                                dataCoding, clientId, mtSourceAddress, mtDestinationAddress, clientPropertyPooler,
                                clientBalancePooler, smsTransactionOperationPooler, rabbitMqPooler,
                                rabbitMqConnection, redisPooler, sessionRef, logger));
                        incomingTrxProcessor.start();
                    }

                    SubmitSmResp submitSmResp = mt.createResponse();
                    if (this.smsTransactionOperationPooler
                            .getUserIsMultiMID(session.getConfiguration().getSystemId())) {
                        // Send submitResponse
                        submitSmResp.setMessageId(tempMessageId);
                        LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived",
                                false, false, true, messageId,
                                "Response UDH should be another message id: " + tempMessageId, null);
                    } else {
                        // Send submitResponse
                        submitSmResp.setMessageId(messageId);
                        LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived",
                                false, false, true, messageId,
                                "Response UDH should be same message id: " + messageId, null);
                    }
                    response = submitSmResp;
                } else {

                    byte dataCoding = mt.getDataCoding();
                    byte[] shortMessage = mt.getShortMessage();

                    if (shortMessage.length == 0) {
                        LoggingPooler.doLog(logger, "INFO", "SimpleSMPPSessionHandler", "firePduRequestReceived", false,
                                false, true, messageId,
                                "Empty short message content. Checking the TLV.", null);

                        // Get TLV message_payload
                        Tlv theTLV = mt.getOptionalParameter(Short.decode("0x0424"));
                        byte[] tlvContent = theTLV.getValue();
                        String strTlvContent = CharsetUtil.decode(tlvContent, "GSM");
                        LoggingPooler.doLog(logger, "INFO", "SimpleSMPPSessionHandler", "firePduRequestReceived", false,
                                false, true, messageId,
                                "The TLV Content: " + strTlvContent, null);

                        // If shortMessage is not defined, use tlvContent
                        shortMessage = tlvContent;
                    }

                    // Run thread smppIncomingTrxProcessor
                    Thread incomingTrxProcessor = new Thread(new SMPPProcessor(messageId, systemId, remoteIpAddress, "",
                            clientSenderId, msisdn, shortMessage,
                            dataCoding, clientId, mtSourceAddress, mtDestinationAddress, clientPropertyPooler,
                            clientBalancePooler, smsTransactionOperationPooler, rabbitMqPooler,
                            rabbitMqConnection, redisPooler, sessionRef, logger));
                    incomingTrxProcessor.start();

                    // Send submitresponse
                    SubmitSmResp submitSmResp = mt.createResponse();
                    submitSmResp.setMessageId(messageId);

                    response = submitSmResp;
                }

            } catch (Exception e) {
                e.printStackTrace();

                LoggingPooler.doLog(logger, "DEBUG", "SimpleSMPPSessionHandler", "firePduRequestReceived", true, false,
                        true, "",
                        "Failed handling SUBMIT_SMS. Error occurs.", e);
            }
        }

        return response;
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
}
