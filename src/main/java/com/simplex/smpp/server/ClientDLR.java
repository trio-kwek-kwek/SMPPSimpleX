package com.simplex.smpp.server;

import com.cloudhopper.commons.util.windowing.WindowFuture;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppServerSession;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.DeliverSmResp;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.util.DeliveryReceipt;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.*;
import com.simplex.smpp.toolpooler.LoggingPooler;
import com.simplex.smpp.toolpooler.RabbitMQPooler;
import com.simplex.smpp.toolpooler.RedisPooler;
import com.simplex.smpp.toolpooler.SMSTransactionOperationPooler;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ClientDLR implements Runnable {
    private static final String DLRQUEUE = "SMPP_DLR";
    private static final int clientDlrTPS = 100;
    ThreadPoolExecutor executorDLRSubmitter = null;
    private Logger logger;
    private RabbitMQPooler rabbitMqPooler;
    private Connection connection;
    private Channel channel;
    private RedisPooler redisPooler;
    private RedisCommands<String, String> redisCommand;
    private SMSTransactionOperationPooler smsTransactionOperationPooler;

    public ClientDLR() {
        try {
            // Setup logger
            logger = LogManager.getLogger("SMPP_SERVER");

            // Initiate LoggingPooler
            new LoggingPooler();

            rabbitMqPooler = new RabbitMQPooler();

            // Initiate redisPooler
            redisPooler = new RedisPooler();
            redisCommand = redisPooler.redisInitiateConnection();

            connection = rabbitMqPooler.getConnection();
            // Initiate rabbitmq channel
            channel = rabbitMqPooler.getChannel(connection);

            channel.queueDeclare(DLRQUEUE, true, false, false, null);

            channel.basicQos(clientDlrTPS);

            // Setup executer
            executorDLRSubmitter = (ThreadPoolExecutor) Executors.newFixedThreadPool(3 * clientDlrTPS);

            smsTransactionOperationPooler = new SMSTransactionOperationPooler();

            LoggingPooler.doLog(logger, "INFO", "ClientDLR", "ClientDLR", false,
                    false, false, "", "Successfully initialize channel rabbitMq to queueName " + DLRQUEUE, null);
        } catch (Exception e) {
            LoggingPooler.doLog(logger, "INFO", "ClientDLR", "ClientDLR", true,
                    false, true, "", "Failed to initialize channle rabbitMq to queueName " + DLRQUEUE, e);
        }
    }

    private void processClientDLR(String queueMessage) {
        LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false, false, true,
                "", "Processing queue DRL with message: " + queueMessage, null);

        sendClientDR drSender = new sendClientDR(queueMessage);
        executorDLRSubmitter.submit(drSender);
    }

    private void readDLRQueue() {
        try {
            LoggingPooler.doLog(logger, "INFO", "ClientDLR", "readDLRQueue", false, false, true, "",
                    "Reading queue " + DLRQUEUE + " for DLR...", null);

            // Guava rateLimiter
            RateLimiter rateLimiter = RateLimiter.create(clientDlrTPS);

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body)
                        throws IOException {
                    String message = new String(body, StandardCharsets.UTF_8);

                    LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "readDLRQueue", false, false,
                            true, "", "Receive message: " + message, null);

                    // Limit the speed
                    rateLimiter.acquire();

                    // Send to client via SMPPServer
                    processClientDLR(message);

                    LoggingPooler.doLog(logger, "INFO", "ClientDLR", "readDLRQueue", false, false,
                            true, "", "Done processing message: " + message, null);

                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            };

            boolean autoAck = false; // If not finally exectued well, no ack to rabbitmq, message not gone
            channel.basicConsume(DLRQUEUE, autoAck, consumer);
        } catch (Exception e) {
            LoggingPooler.doLog(logger, "INFO", "ClientDLR", "readDLRQueue", true, false, false, "",
                    "Failed to access queue " + DLRQUEUE, e);

            // Re-initiate channel
            channel = rabbitMqPooler.getChannel(connection);
        }
    }

    @Override
    public void run() {
        try {
            LoggingPooler.doLog(logger, "INFO", "ClientDLR", "RUN", false, false, true, "",
                    "Starting DLR Submitter.", null);

            readDLRQueue();

            LoggingPooler.doLog(logger, "INFO", "ClientDLR", "RUN", false, false, true, "",
                    "Done reading queue 1", null);
        } catch (Exception e) {
            e.printStackTrace();

            LoggingPooler.doLog(logger, "INFO", "ClientDLR", "RUN", true, false, true, "",
                    "Failed to start Client DLR Submitter. Error occurs.", e);
        }
    }


    // Sending DR thread
    public class sendClientDR implements Runnable {
        private final String queueMessage;

        public sendClientDR(String theQueueMessage) {
            this.queueMessage = theQueueMessage;
        }

        @SuppressWarnings("rawtypes")
        private void sendRequestPdu(SmppSession session, String messageId, DeliverSm deliver) {
            try {
                WindowFuture<Integer, PduRequest, PduResponse> future = session.sendRequestPdu(deliver, 10000, false);

                String clientResp = "";
                if (!future.await()) {
                    clientResp = "Failed to receive DELIVER_SM_RESP within specified time";
                } else if (future.isSuccess()) {
                    DeliverSmResp deliverSmResp = (DeliverSmResp) future.getResponse();
                    clientResp = "deliver_sm_resp: commandStatus [" + deliverSmResp.getCommandStatus() + "=" +
                            deliverSmResp.getResultMessage() + "]";
                } else {
                    clientResp = "Failed to properly receive deliver_sm_resp: " + future.getCause();
                }
                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "sendRequestPdu", false, false,
                        false, "", clientResp, null);

                // Update transaction DLR for clientResponse
                smsTransactionOperationPooler.insertTransactionDLRClientResponse(messageId, clientResp);
                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "sendRequestPdu", false, false,
                        false, "", "Client response is saved to table transacion_sms_dlr.", null);
            } catch (Exception e) {
                e.printStackTrace();
                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "sendRequestPdu", true, false,
                        false, "", "Failed to send PDU to client. Error occured.", e);
            }
        }

        private void sendDeliveryReceipt(SmppSession session, String messageId, Address mtDestinationAddress,
                                         Address mtSourceAddress, byte[] shortMessage, byte dataCoding) {

            try {
                DeliverSm deliver = new DeliverSm();
                deliver.setEsmClass(SmppConstants.ESM_CLASS_MT_SMSC_DELIVERY_RECEIPT);
                deliver.setSourceAddress(mtDestinationAddress);
                deliver.setDestAddress(mtSourceAddress);
                deliver.setDataCoding(dataCoding);
                deliver.setShortMessage(shortMessage);

                sendRequestPdu(session, messageId, deliver);

            } catch (Exception e) {
                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "sendRequestPdu", true, false,
                        false, "", "Failed sending delivery report.", e);
            }
        }


        private void proccessClientDLRInNewTread() {
            LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false, false,
                    true, "", "Processing queue DRL with message: " + queueMessage, null);

            try {
                JSONObject jsonMessage = new JSONObject(queueMessage);

                String messageId = jsonMessage.getString("messageId");
                //String status = jsonMessage.getString("errorCode");
                String status = jsonMessage.getString("status");
                String msisdn = jsonMessage.getString("msisdn");
                String originMessageId = "";
                if (jsonMessage.has("originMessageId")) {
                    originMessageId = jsonMessage.getString("originMessageId");
                }

                // Get sessionId yang digunakan utk terima message, spy DR menggunakan session yang sama
                String sysSessionId = "";
                if (jsonMessage.has("sysSessionId")) {
                    sysSessionId = jsonMessage.getString("sysSessionId");
                }

                // Get message and systemId and clientSenderId
                String redisKey = "trxdata-" + messageId.trim();
                String redisVal = redisPooler.redisGet(redisCommand, redisKey);

                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false, false,
                        false, messageId, "messageId: " + messageId + ", status: " + status + ", msisdn: " + msisdn +
                                ", trxdata value " + redisVal, null);

                JSONObject jsonTrxDetail = smsTransactionOperationPooler.getTransactionDetail(messageId);

                if (!jsonTrxDetail.has("message")) {
                    jsonTrxDetail = smsTransactionOperationPooler.getTransactionDetail(originMessageId);
                }

                String theSMS = jsonTrxDetail.getString("message");
                String theSysId = jsonTrxDetail.getString("apiUserName").trim();
                String theClientSenderId = jsonTrxDetail.getString("clientSenderId").trim();
                String clientId = jsonTrxDetail.getString("clientId");
                String encoding = jsonTrxDetail.getString("encoding");

                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false, false,
                        false, messageId, "messageId: " + messageId + ", theSMS: " + theSMS, null);

                // Get the SMPPSession
                SmppServerSession theSession = null;

                // get multi session but will anomaly DLR send different connection if their multi connection same system id
                if (SMPPServer.mapSession.containsKey(sysSessionId)) {
                    theSession = SMPPServer.mapSession.get(sysSessionId);
                } else {
                    // Cari mapSession dengan sysSessionId starts with systemId
                    for (Entry<String, SmppServerSession> entry : SMPPServer.mapSession.entrySet()) {
                        if (entry.getKey().startsWith(theSysId)) {
                            theSession = entry.getValue();
                        }
                    }
                }

                if (theSession != null) {
                    LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false,
                            false, false, messageId, "Preparing session: " + theSession.getConfiguration().getName() +
                                    ". isBound: " + theSession.isBound(), null);

                    boolean isMultiMID = smsTransactionOperationPooler.getUserIsMultiMID(theSysId);

                    if (isMultiMID) {
                        String[] strMultipartMesseageIds = jsonTrxDetail.getString("multipart_messeage_ids").split(",");
                        int deliveredCount = 0;
                        if (strMultipartMesseageIds.length > 0 && !jsonTrxDetail.getString("multipart_messeage_ids").isEmpty()) {
                            for (String messageIdPart : strMultipartMesseageIds) {
                                messageId = messageIdPart.trim();
                                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR",
                                        false, false, false, messageId, "DLR sysId: " + theSysId +
                                                " -> found matching SMPPSession: " +
                                                theSession.getConfiguration().getName(), null);

                                // Prepare the DLR
                                int submitCount = 1;
                                byte deliveryState = SmppConstants.STATE_DELIVERED;
                                int esmeErrCode = SmppConstants.STATUS_OK;
                                switch (status) {
                                    case "000":
                                        deliveredCount = deliveredCount + 1;
                                        break;
                                    case "002":
                                        deliveryState = SmppConstants.STATE_ACCEPTED;
                                        break;
                                    case "105":
                                        deliveryState = SmppConstants.STATE_UNDELIVERABLE;
                                        esmeErrCode = SmppConstants.STATUS_INVDSTADR;
                                        break;
                                    default:
                                        deliveryState = SmppConstants.STATE_REJECTED;
                                        esmeErrCode = SmppConstants.STATUS_DELIVERYFAILURE;
                                        break;
                                }

                                DeliveryReceipt dlrReceipt = new DeliveryReceipt(messageId, submitCount, deliveredCount,
                                        new DateTime(), new DateTime(), deliveryState, esmeErrCode, theSMS);

                                // Save to DB transaction_sms_dlr - saving db has to be before sendDeliveryReceipt
                                smsTransactionOperationPooler.saveTransactionDLR(messageId, clientId, LocalDateTime.now(),
                                        dlrReceipt.toShortMessage(), status, "SMPP session name " +
                                                theSession.getConfiguration().getName());
                                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR",
                                        false, false, false, messageId, "Data DLR saved in transaction_sms_dlr.", null);

                                Address moSourceAddress = new Address((byte) 0x01, (byte) 0x01, msisdn);
                                Address moDestinationAddress = new Address((byte) 0x03, (byte) 0x00, theClientSenderId);
                                byte dataCoding = (byte) 0x00;
                                if (encoding.equals("UCS2")) {
                                    dataCoding = (byte) 0x08;
                                }

                                // Send DLR
                                sendDeliveryReceipt(theSession, messageId, moSourceAddress, moDestinationAddress,
                                        dlrReceipt.toShortMessage().getBytes(), dataCoding);
                                LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR",
                                        false, false, false, messageId, "Sending DLR Part (" + deliveredCount +
                                                ") with session: " + theSession.getConfiguration().getName() + ". DLR: " +
                                                dlrReceipt.toShortMessage(), null);
                            }
                        } else {
                            LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoClientDLRSubmitter", "proccessClientDLR", false, false, false, messageId,
                                    "DLR sysId: " + theSysId + " -> found matching SMPPSession: " + theSession.getConfiguration().getName(), null);

                            // Prepare the DLR
                            int submitCount = 0;
//                            int deliveredCount = 0;
                            byte deliveryState = SmppConstants.STATE_DELIVERED;
                            int esmeErrCode = SmppConstants.STATUS_OK;
                            switch (status) {
                                case "000":
                                    submitCount = 1;
                                    deliveredCount = 1;
                                    break;
                                case "002":
                                    submitCount = 1;
                                    deliveryState = SmppConstants.STATE_ACCEPTED;
                                    break;
                                case "105":
                                    deliveryState = SmppConstants.STATE_UNDELIVERABLE;
                                    esmeErrCode = SmppConstants.STATUS_INVDSTADR;
                                    break;
                                default:
                                    submitCount = 1;
                                    deliveryState = SmppConstants.STATE_REJECTED;
                                    esmeErrCode = SmppConstants.STATUS_DELIVERYFAILURE;
                                    break;
                            }

                            DeliveryReceipt dlrReceipt = new DeliveryReceipt(messageId, submitCount, deliveredCount, new DateTime(), new DateTime(), deliveryState, esmeErrCode, theSMS);

                            // Save to DB transaction_sms_dlr - saving db has to be before sendDeliveryReceipt
                            smsTransactionOperationPooler.saveTransactionDLR(messageId, clientId, LocalDateTime.now(), dlrReceipt.toShortMessage(), status, "SMPP session name " + theSession.getConfiguration().getName());
                            LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoClientDLRSubmitter", "proccessClientDLR", false, false, false, messageId,
                                    "Data DLR saved in transaction_sms_dlr.", null);

                            Address moSourceAddress = new Address((byte) 0x01, (byte) 0x01, msisdn);
                            Address moDestinationAddress = new Address((byte) 0x03, (byte) 0x00, theClientSenderId);
                            byte dataCoding = (byte) 0x00;
                            if (encoding.equals("UCS2")) {
                                dataCoding = (byte) 0x08;
                            }

                            // Send DLR
                            sendDeliveryReceipt(theSession, messageId, moSourceAddress, moDestinationAddress, dlrReceipt.toShortMessage().getBytes(), dataCoding);
                            LoggingPooler.doLog(logger, "DEBUG", "BlastMeNeoClientDLRSubmitter", "proccessClientDLR", false, false, false, messageId,
                                    "Sending DLR with session: " + theSession.getConfiguration().getName() + ". DLR: " + dlrReceipt.toShortMessage(), null);
                        }
                    } else {
                        LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false,
                                false, false, messageId, "DLR sysId: " + theSysId + " -> found matching SMPPSession: " +
                                        theSession.getConfiguration().getName(), null);

                        // Prepare the DLR
                        int submitCount = 0;
                        int deliveredCount = 0;
                        byte deliveryState = SmppConstants.STATE_DELIVERED;
                        int esmeErrCode = SmppConstants.STATUS_OK;
                        switch (status) {
                            case "000":
                                submitCount = 1;
                                deliveredCount = 1;
                                break;
                            case "002":
                                submitCount = 1;
                                deliveryState = SmppConstants.STATE_ACCEPTED;
                                break;
                            case "105":
                                deliveryState = SmppConstants.STATE_UNDELIVERABLE;
                                esmeErrCode = SmppConstants.STATUS_INVDSTADR;
                                break;
                            default:
                                submitCount = 1;
                                deliveryState = SmppConstants.STATE_REJECTED;
                                esmeErrCode = SmppConstants.STATUS_DELIVERYFAILURE;
                                break;
                        }

                        DeliveryReceipt dlrReceipt = new DeliveryReceipt(messageId, submitCount, deliveredCount,
                                new DateTime(), new DateTime(), deliveryState, esmeErrCode, theSMS);

                        // Save to DB transaction_sms_dlr - saving db has to be before sendDeliveryReceipt
                        smsTransactionOperationPooler.saveTransactionDLR(messageId, clientId, LocalDateTime.now(),
                                dlrReceipt.toShortMessage(), status, "SMPP session name " +
                                        theSession.getConfiguration().getName());
                        LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false,
                                false, false, messageId, "Data DLR saved in transaction_sms_dlr.", null);

                        Address moSourceAddress = new Address((byte) 0x01, (byte) 0x01, msisdn);
                        Address moDestinationAddress = new Address((byte) 0x03, (byte) 0x00, theClientSenderId);
                        byte dataCoding = (byte) 0x00;
                        if (encoding.equals("UCS2")) {
                            dataCoding = (byte) 0x08;
                        }

                        // Send DLR
                        sendDeliveryReceipt(theSession, messageId, moSourceAddress, moDestinationAddress,
                                dlrReceipt.toShortMessage().getBytes(), dataCoding);
                        LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false,
                                false, false, messageId, "Sending DLR with session: " +
                                        theSession.getConfiguration().getName() + ". DLR: " +
                                        dlrReceipt.toShortMessage(), null);
                    }
                    //theSession.close();
                } else {
                    LoggingPooler.doLog(logger, "DEBUG", "ClientDLR", "proccessClientDLR", false,
                            false, false, messageId, "CAN NOT FIND MATCHING SESSIONID FOR THE DLR. IGNORE THE DLR.",
                            null);
                }
            } catch (Exception e) {
                LoggingPooler.doLog(logger, "INFO", "ClientDLR", "processDlrQueue", true, false,
                        true, "", "Failed to process the DLR message.", e);
            }
        }


        @Override
        public void run() {
            proccessClientDLRInNewTread();
        }
    }
}
