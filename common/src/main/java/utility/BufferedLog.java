package utility;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BufferedLog implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final String file;
    private final boolean logToConsole = true;

    private final BlockingQueue<String> logQueue = new LinkedBlockingQueue<>();
    private transient Thread writerThread;
    private volatile boolean running = true;

    public enum RequestResponseStatus implements Serializable {
        SUCCESS, FAILURE, DEBUG, INFO;

        @Override
        public String toString() {
            return switch (this) {
                case SUCCESS -> "Success";
                case FAILURE -> "Failure";
                case DEBUG -> "Debug";
                case INFO -> "Info";
            };
        }
    }

    public enum LogLevel {
        INFO, DEBUG, ERROR
    }

    public BufferedLog(String destination, String fileName, String host) {
        this.file = destination + fileName + "_log.log";
        this.host = host;
        startWriterThread();
    }

    private void startWriterThread() {
        writerThread = new Thread(() -> {
            while (running || !logQueue.isEmpty()) {
                try {
                    String entry = logQueue.take();
                    writeToFile(entry);
                } catch (InterruptedException ignored) {
                }
            }
        });
        writerThread.setDaemon(true);
        writerThread.start();
    }

    private void writeToFile(String logEntry) {
        try {
            File logFile = new File(file);
            logFile.getParentFile().mkdirs();
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true))) {
                writer.write(logEntry);
                writer.newLine();
                writer.flush();
            }
        } catch (IOException e) {
            System.err.println("Failed to write to log file: " + e.getMessage());
        }
    }

    public void logEntry(
            String requestedFrom,
            String requestDetails,
            RequestResponseStatus status,
            String response,
            String message) {

        logEntry(requestedFrom, requestDetails, status, response, message, LogLevel.INFO);
    }

    public void logEntry(
            String requestedFrom,
            String requestDetails,
            RequestResponseStatus status,
            String response,
            String message,
            LogLevel level) {

        String createdTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String logEntry = String.format(
                "[%s] [%s] Host: %s\t| Requested From: %s\t| Message: %s\t| Request Details: %s\t| Status: %s\t| Response: %s",
                level, createdTime, host, requestedFrom, message, requestDetails, status, response
        );

        logQueue.offer(logEntry);

        if (logToConsole) {
            System.out.println(logEntry);
        }
    }

    public void shutdown() {
        running = false;
        writerThread.interrupt();
    }
}
