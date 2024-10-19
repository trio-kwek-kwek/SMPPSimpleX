package com.simplex.smpp.server;

import at.favre.lib.crypto.bcrypt.BCrypt;
import com.cloudhopper.smpp.*;
import com.cloudhopper.smpp.impl.DefaultSmppServer;
import com.cloudhopper.smpp.pdu.BaseBind;
import com.cloudhopper.smpp.pdu.BaseBindResp;
import com.cloudhopper.smpp.type.SmppChannelException;
import com.cloudhopper.smpp.type.SmppProcessingException;
import com.rabbitmq.client.Connection;
import com.simplex.smpp.configuration.Configuration;
import com.simplex.smpp.toolpooler.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SMPPServer {
    private final static int smppPort = 4775;
    private final static int smppMaxConnection = 500;
    private final static int smppRequestExpiryTimeout = 30000;
    private final static int smppWindowMonitorInterval = 15000;
    private final static int smppWindowSize = 10;
    static HashMap<String, SmppServerSession> mapSession = new HashMap<String, SmppServerSession>();
    private static Logger logger;
    private static RedisPooler redisPooler;

    public SMPPServer() {
        // Set timezone
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Jakarta"));

        // Load Configuration
        new Configuration();
//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());

        // Setup logger
        logger = LogManager.getLogger("SMPP_SERVER");

        try {
            // Initiate RedisPooler
            redisPooler = new RedisPooler();

            // Initiate RabbitMQPooler
            new RabbitMQPooler();

            // Initiate jsonPrefixProperty in TelecomPrefixPooler
            new TelecomPrefixPooler();

            // Initiate SenderIdSMSPooler
            new SenderIdSMSPooler();

            // Initiate RouteSMSPooler
            new RouteSMSPooler();
        } catch (Exception e) {
            e.printStackTrace();

            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        SMPPServer smppServer = new SMPPServer();
        smppServer.startSMPPServer();
    }

    private void startSMPPServer() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        try {
            // create a server configuration
            SmppServerConfiguration configuration = new SmppServerConfiguration();
            configuration.setPort(smppPort);
            configuration.setMaxConnectionSize(smppMaxConnection);
            configuration.setNonBlockingSocketsEnabled(true);
            configuration.setDefaultRequestExpiryTimeout(smppRequestExpiryTimeout);
            configuration.setDefaultWindowMonitorInterval(smppWindowMonitorInterval);
            configuration.setDefaultWindowSize(smppWindowSize);
            configuration.setDefaultWindowWaitTimeout(configuration.getDefaultRequestExpiryTimeout());
            configuration.setDefaultSessionCountersEnabled(true);
            configuration.setJmxEnabled(true);

            DefaultSmppServer smppServer = new DefaultSmppServer(configuration, new SimpleSmppServerHandler(), executor);

            logger.info("Starting SIMPLE SMPP server...");
            smppServer.start();
            logger.info("SIMPLE SMPP server is started");
        } catch (SmppChannelException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static class SimpleSmppServerHandler implements SmppServerHandler {
        private final static ScheduledThreadPoolExecutor execClientDLR = new ScheduledThreadPoolExecutor(5);
        private UserAPISMPPSMSPooler userApiSMPPSMSPooler;
        private SMPPEnquiryLinkPooler smppEnquiryLinkPooler;
        private ClientBalancePooler clientBalancePooler;
        private SMSTransactionOperationPooler smsTransactionOperationPooler;
        private ClientPropertyPooler clientPropertyPooler;
        private RabbitMQPooler rabbitPooler;
        private Connection rabbitConnection;
        private Tool tool;

        public SimpleSmppServerHandler() {
            try {
                tool = new Tool();    // Initiate tool

                userApiSMPPSMSPooler = new UserAPISMPPSMSPooler();
                System.out.println("USERAPISMPPSMSPooler is initiated.");

                smppEnquiryLinkPooler = new SMPPEnquiryLinkPooler();
                System.out.println("SMPPEnquiryLinkPooler is initiated.");

                clientBalancePooler = new ClientBalancePooler();
                System.out.println("ClientBalancePooler is initiated.");

                smsTransactionOperationPooler = new SMSTransactionOperationPooler();
                System.out.println("SMSTransactionPooler is initiated.");

                clientPropertyPooler = new ClientPropertyPooler();
                System.out.println("ClientPropertyPooler is initiated.");

                rabbitPooler = new RabbitMQPooler();
                System.out.println("RabbitMQPooler is initiated.");

                rabbitConnection = rabbitPooler.getConnection();
                System.out.println("RabbitMQConnection is initiated.");

                System.out.println("Executing CLIENTDLRSUBMITTER.");    // Run executor DLR Client
                execClientDLR.schedule(new ClientDLR(), 10, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();

                LoggingPooler.doLog(logger, "DEBUG", "SMPPServer", "SimpleSMPPServerHandler", true, false, true, "",
                        "Failed to initiate SimpleSMPPServerHandler. Error occur.", e);
            }
        }

        @Override
        public void sessionBindRequested(
                Long sessionId, SmppSessionConfiguration sessionConfiguration,
                BaseBind bindRequest) throws SmppProcessingException {


            String systemId = sessionConfiguration.getSystemId();
            String password = sessionConfiguration.getPassword();
            String remoteIpAddress = sessionConfiguration.getHost();

            LoggingPooler.doLog(logger, "DEBUG", "SMPPServer - DefaultSmppServerHandler", "sessionBindRequested", false, false, true, "",
                    "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress, null);

            // Validate username and password
            if (UserAPISMPPSMSPooler.jsonSimpleSysIdAccess.has(systemId.trim())) {
                LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionBindRequested", false, false, true, "",
                        "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", SYSTEMID IS VERIFIED.", null);

                // systemId valid, check password
                JSONObject jsonDetail = UserAPISMPPSMSPooler.jsonSimpleSysIdAccess.getJSONObject(systemId.trim());

                String configPassword = jsonDetail.getString("password");
                String configIPAddress = jsonDetail.getString("ipAddress");

                if (configIPAddress.trim().contains("ALL") || configIPAddress.trim().contains(remoteIpAddress.trim())) {
                    // Remote IP Address is VALID.
                    LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionBindRequested", false, false, true, "",
                            "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", REMOTE IP ADDRESS IS VERIFIED.", null);

                    // Verify the password
                    BCrypt.Result result = BCrypt.verifyer().verify(password.getBytes(StandardCharsets.UTF_8), configPassword.getBytes());

                    if (result.verified) {
                        // Check for existing session
                        SmppServerSession existingSession = mapSession.get(systemId);
                        if (existingSession == null || (existingSession != null && !existingSession.isBound())) {
                            // PASSWORD VERIFIED
                            LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionBindRequested", false, false, true, "",
                                    "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", PASSWORD IS VERIFIED.", null);

                            // LOGIN SUCCESS
                            smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "PROCESSING");

                            // If there is an existing session but not bound, remove it
                            if (existingSession != null) {
                                existingSession.destroy();
                                mapSession.remove(systemId);
                                LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionBindRequested", false, false, true, "",
                                        "Existing session closed and removed for systemId: " + systemId, null);
                            }
                        } else {
                            // Close the existing session
                            LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionBindRequested", false, false, true, "",
                                    "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", SYSTEM ID IS ALREADY CONNECT, NO DUPLICATE BIND.", null);
                            // Save to SMPP bind attempt to log
                            smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "FAILED TO BIND. SYSTEM ID IS ALREADY CONNECT, NO DUPLICATE BIND.");

                            // Throw invalid BIND FAILED
                            throw new SmppProcessingException(SmppConstants.STATUS_BINDFAIL, null);
                        }
                    } else {
                        // NOT VERIFIED
                        LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionBindRequested", false, false, true, "",
                                "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", PASSWORD IS INVALID.", null);

                        // Save to SMPP bind attempt to log
                        smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "FAILED TO BIND. INVALID SYSID/PASSWORD/REMOTE IP ADDRESS.");

                        // Throw invalid password
                        throw new SmppProcessingException(SmppConstants.STATUS_INVPASWD, null);
                    }
                } else {
                    // Remote IP Address INVALID
                    LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionBindRequested", false, false, true, "",
                            "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", REMOTE IP ADDRESS IS INVALID.", null);

                    // Save to SMPP bind attempt to log
                    smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "FAILED TO BIND. INVALID SYSID/PASSWORD/REMOTE IP ADDRESS.");

                    // Throw invalid password
                    throw new SmppProcessingException(SmppConstants.STATUS_INVSYSID, null);
                }
            } else {
                // SystemID INVALID
                LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionBindRequested", false, false, true, "",
                        "Incoming binding request - systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", SYSTEMID IS INVALID.", null);

                // Save to SMPP bind attempt to log
                smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING REQUESTED", "FAILED TO BIND. INVALID SYSID/PASSWORD/REMOTE IP ADDRESS.");

                // Throw invalid password
                throw new SmppProcessingException(SmppConstants.STATUS_INVSYSID, null);
            }
        }

        @Override
        public void sessionCreated(Long sessionId, SmppServerSession session, BaseBindResp preparedBindResponse)
                throws SmppProcessingException {

            LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionCreated", false, false, true, "",
                    "Session created: " + sessionId.toString(), null);

            String systemId = session.getConfiguration().getSystemId();
            String remoteIpAddress = session.getConfiguration().getHost();
            String clientId = userApiSMPPSMSPooler.getSimpleClientId(systemId);
            LoggingPooler.doLog(logger, "INFO", "SimpleSmppServerHandler", "sessionCreated", false, false, true, "",
                    "systemId: " + systemId + ", remote IP address: " + remoteIpAddress + ", clientID: " + clientId, null);

            // Assign sessionId
//            String SessionId = systemId + "-" + tool.generateUniqueID();
//            LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionCreated", false, false, true, "",
//					"SessionId: " + SessionId, null);

            // Put session into mapSession
            mapSession.put(systemId, session);

            // Name the session with SessionId
            session.getConfiguration().setName(systemId);

            // Save to smpp bind attempt log
            smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), systemId, remoteIpAddress, "BINDING SESSION CREATED", "SUCCESS - SESSION CREATED");

            LoggingPooler.doLog(logger, "DEBUG", "SimpleSmppServerHandler", "sessionCreated", false, false, true, "",
                    "Session created fro systemId: " + systemId + ", remoteIpAddress: " + remoteIpAddress + ", clientId: " +
                            clientId + " -> SessionId: " + systemId + ". Session NAME: " + session.getConfiguration().getName(), null);

            session.serverReady(new SMPPSessionHandler(session, systemId, remoteIpAddress, clientId, tool, smppEnquiryLinkPooler, clientPropertyPooler,
                    clientBalancePooler, smsTransactionOperationPooler, rabbitPooler, rabbitConnection, redisPooler, logger));
        }

        @Override
        public void sessionDestroyed(Long sessionId, SmppServerSession session) {
            LoggingPooler.doLog(logger, "INFO", "SimpleSmppServerHandler", "sessionDestroyed", false, false, true, "",
                    "Session destroyed: " + session.toString(), null);

            // print out final stats
            if (session.hasCounters()) {
                logger.info(" final session rx-submitSM: {}", session.getCounters().getRxSubmitSM());
            }

            // Save to smpp bind attempt log
            smsTransactionOperationPooler.saveSMPPBindAttempt(String.valueOf(sessionId), LocalDateTime.now(), session.getConfiguration().getSystemId(),
                    session.getConfiguration().getHost(), "BINDING DESTROY", "SUCCESS - SESSION DESTROYED");

            // make sure it's really shutdown
            session.destroy();
        }
    }


}
