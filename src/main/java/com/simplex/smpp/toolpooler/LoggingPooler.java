package com.simplex.smpp.toolpooler;

import com.simplex.smpp.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LoggingPooler {
    private static StringWriter sw;

    public LoggingPooler() {
        new Configuration();
//        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
//        File file = new File(Configuration.getLogConfigPath());
//        context.setConfigLocation(file.toURI());
        Logger logger = LogManager.getLogger("LOGGINGPOOLER");

        sw = new StringWriter();

        logger.debug("Module LoggingPooler is initiated ... ");
    }

    public static void doLog(Logger origLogger, String level, String moduleName, String functionName, Boolean isError, Boolean isWarning, Boolean isConsole, String GUID,
                             String message, Exception errorCause) {
        // prepare the GUID
        if ((!GUID.isEmpty()) && !GUID.trim().equals("-")) {
            GUID = GUID + " - ";
        } else {
            GUID = "";
        }

        String originalMessage = message;
        String stackedMessage = message;
        if (errorCause != null) {
            errorCause.printStackTrace(new PrintWriter(sw));

            message = message + ". Exception: " + errorCause;

            stackedMessage = message + ". Stack Trace: " + sw.toString();
        }

        switch (level) {
            case "TRACE":
                origLogger.trace(moduleName + " - " + functionName + " - " + GUID + stackedMessage);
                origLogger.debug(moduleName + " - " + functionName + " - " + GUID + stackedMessage);

                if (isError) {
                    origLogger.error(GUID + message);
                }

                if (isWarning) {
                    origLogger.warn(GUID + message);
                }

                if (isConsole) {
                    System.out.println(GUID + message);

                    if (errorCause != null) {
                        errorCause.printStackTrace();
                    }
                }
                break;
            case "DEBUG":
                origLogger.debug(moduleName + " - " + functionName + " - " + GUID + stackedMessage);

                if (isError) {
                    origLogger.error(GUID + message);
                }

                if (isWarning) {
                    origLogger.warn(GUID + message);
                }

                if (isConsole) {
                    System.out.println(GUID + message);

                    if (errorCause != null) {
                        errorCause.printStackTrace();
                    }
                }
                break;
            case "INFO":
                origLogger.debug(moduleName + " - " + functionName + " - " + GUID + stackedMessage);
                origLogger.info(GUID + originalMessage);

                if (isError) {
                    origLogger.error(GUID + message);

                    if (errorCause != null) {
                        errorCause.printStackTrace();
                    }
                }

                if (isWarning) {
                    origLogger.warn(GUID + message);
                }

                if (isConsole) {
                    System.out.println(GUID + message);

                    if (errorCause != null) {
                        errorCause.printStackTrace();
                    }
                }
                break;
            case "ERROR":
                origLogger.trace(moduleName + " - " + functionName + " - " + GUID + stackedMessage);
                origLogger.debug(moduleName + " - " + functionName + " - " + GUID + stackedMessage);
                origLogger.info(GUID + originalMessage);
                origLogger.error(GUID + originalMessage);

                if (isError) {
                    origLogger.error(GUID + message);

                    if (errorCause != null) {
                        errorCause.printStackTrace();
                    }
                }

                if (isWarning) {
                    origLogger.warn(GUID + message);
                }

                if (isConsole) {
                    System.out.println(GUID + message);

                    if (errorCause != null) {
                        errorCause.printStackTrace();
                    }
                }
                break;
        }
    }
}
