import io.github.cdimascio.dotenv.Dotenv;
import network.UDPMessage;
import utility.BufferedLog;

import java.io.*;
import java.net.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class FrontEnd {
    private static final Dotenv dotenv = Dotenv.configure()
            .directory(Paths.get(System.getProperty("user.dir")).toString())
            .load();
    private final List<InetSocketAddress> replicaManagerEndpoints;
    private final DatagramSocket socket;
    private final BlockingQueue<Message> receiveMessage = new LinkedBlockingQueue<>();
    private final BlockingQueue<SendMessage> sentMessages = new LinkedBlockingQueue<>();
    private final BlockingQueue<ClientRequest> sequencerQueue = new LinkedBlockingQueue<>();
    private static final ExecutorService taskExecutor = Executors.newFixedThreadPool(10);
    private static BufferedLog log;

    private static long ACK_TIMEOUT;
    private static long REPLICA_RESPONSE_TIMEOUT;

    public FrontEnd(List<InetSocketAddress> replicaManagerEndpoints) throws SocketException {
        log = new BufferedLog("logs/", "Front-End", "Front-End");
        this.replicaManagerEndpoints = replicaManagerEndpoints;
        ACK_TIMEOUT = Long.parseLong(dotenv.get("ACK_TIMEOUT"));
        REPLICA_RESPONSE_TIMEOUT = Long.parseLong(dotenv.get("REPLICA_RESPONSE_TIMEOUT"));
        this.socket = new DatagramSocket(Integer.parseInt(dotenv.get("FE_PORT")));
        log.logEntry("Constructor", "Initialization complete", BufferedLog.RequestResponseStatus.SUCCESS,
                "FE listening on port " + dotenv.get("FE_PORT"), "Front-End started");
    }

    private class Message {
        final UDPMessage message;
        long timeSent;
        final InetSocketAddress endpoint;

        public Message(UDPMessage message, InetSocketAddress endpoint) {
            this.message = message;
            this.timeSent = System.currentTimeMillis();
            this.endpoint = endpoint;
        }
    }

    private class SendMessage extends Message {
        boolean isSend;

        public SendMessage(UDPMessage message, InetSocketAddress endpoint) {
            super(message, endpoint);
            this.isSend = false;
        }

        public SendMessage(Message message) {
            super(message.message, message.endpoint);
            this.isSend = false;
        }

        public void sent() {
            this.isSend = true;
        }

        public void reSend() {
            this.isSend = false;
            this.timeSent = System.currentTimeMillis();
            log.logEntry("FE_SendLoop", "Re-sending Message", BufferedLog.RequestResponseStatus.DEBUG,
                    "MessageID: " + message.getMessageId(), "Resend triggered for endpoint " + endpoint);
        }
    }

    private class ClientRequest {
        final Long sequenceNumber;
        long timestamp;
        HashMap<InetAddress, String> replicaResponses;
        boolean isTimeOut = false;
        boolean isSendToClient = false;
        boolean isSendToServer = false;
        final UDPMessage originalMessage;
        final InetSocketAddress clientInetSocketAddress;

        public ClientRequest(Long sequenceNumber, UDPMessage originalMessage, InetSocketAddress endpoint) {
            this.sequenceNumber = sequenceNumber;
            this.timestamp = System.currentTimeMillis();
            this.replicaResponses = new HashMap<>();
            this.originalMessage = originalMessage;
            this.clientInetSocketAddress = endpoint;
            log.logEntry("FE_ClientRequest", "New Request", BufferedLog.RequestResponseStatus.INFO,
                    "Sequence: " + sequenceNumber, "Created client request for endpoint " + endpoint);
        }

        public void addResponse(InetAddress address, String response) {
            if (!isFull()) {
                log.logEntry("FE_ClientRequest", "Response Added", BufferedLog.RequestResponseStatus.SUCCESS,
                        "From " + address.getHostAddress() + " Response: " + response, "Sequence: " + sequenceNumber);
                replicaResponses.put(address, response);
                if (isSendToClient) {
                    isSendToClient = false;
                }
                if (System.currentTimeMillis() - this.timestamp > Long.parseLong(dotenv.get("REPLICA_RESPONSE_TIMEOUT"))) {
                    this.timestamp = System.currentTimeMillis();
                }
            }
        }

        public boolean isFull() {
            return replicaResponses.size() == 3;
        }

        private boolean isTimeOut() {
            return this.isTimeOut;
        }

        private void timeOut() {
            this.isTimeOut = true;
            log.logEntry("FE_ClientRequest", "Request Timeout", BufferedLog.RequestResponseStatus.FAILURE,
                    "Sequence: " + sequenceNumber, "Request timed out");
        }

        public void sendToClient() {
            this.isSendToClient = true;
        }

        public void sendToServer() {
            this.isSendToServer = true;
        }

        public ReplicateResponseMetric analyzeResponses() {
            Map<String, List<InetAddress>> groupedByResult = new HashMap<>();

            for (Map.Entry<InetAddress, String> entry : replicaResponses.entrySet()) {
                String result = entry.getValue();
                InetAddress address = entry.getKey();
                groupedByResult.computeIfAbsent(result, k -> new ArrayList<>()).add(address);
            }

            String majorityResult = null;
            List<InetAddress> matched = new ArrayList<>();

            for (Map.Entry<String, List<InetAddress>> entry : groupedByResult.entrySet()) {
                if (entry.getValue().size() >= 2) {
                    majorityResult = entry.getKey();
                    matched = new ArrayList<>(entry.getValue());
                    break;
                }
            }

            List<InetAddress> mismatched = new ArrayList<>();
            if (majorityResult != null && matched.size() != replicaResponses.size()) {
                for (Map.Entry<InetAddress, String> entry : replicaResponses.entrySet()) {
                    if (!entry.getValue().equals(majorityResult)) {
                        mismatched.add(entry.getKey());
                    }
                }
            } else if (majorityResult == null) {
                mismatched.addAll(replicaResponses.keySet());
            }

            if (majorityResult != null) {
                log.logEntry("FE_ClientRequest", "Analyze Responses", BufferedLog.RequestResponseStatus.INFO,
                        "Majority: " + majorityResult, "Matched: " + matched + " Mismatched: " + mismatched);
            }
            return new ReplicateResponseMetric(majorityResult, matched, mismatched);
        }
    }

    private record ReplicateResponseMetric(String majorityResult, List<InetAddress> matchedInetAddress,
                                           List<InetAddress> errorInetAddress) {
    }

    private void sendMessage(SendMessage sendMessage) {
        try {
            byte[] buffer = sendMessage.message.serialize();
            InetAddress receiverAddress = sendMessage.endpoint.getAddress();
            int receiverPort = sendMessage.endpoint.getPort();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, receiverAddress, receiverPort);
            socket.send(packet);
            log.logEntry("FE_SendLoop", "Message Sent", BufferedLog.RequestResponseStatus.SUCCESS,
                    "MessageID: " + sendMessage.message.getMessageId(), "Sent to " + sendMessage.endpoint);
            System.out.println("Send message to " + receiverAddress + ":" + receiverPort + " " + sendMessage.message);
        } catch (Exception e) {
            log.logEntry("FE_SendLoop", "Error Sending Message", BufferedLog.RequestResponseStatus.FAILURE,
                    e.getMessage(), "Endpoint: " + sendMessage.endpoint);
            e.printStackTrace(System.err);
        }
    }

    public void start() {
        new Thread(this::receiveLoop).start();
        new Thread(this::sendLoop).start();
        new Thread(this::processIncomingMessages).start();
        new Thread(this::checkResultFound).start();
        log.logEntry("FE_Start", "Front-End started", BufferedLog.RequestResponseStatus.SUCCESS, "All threads launched", "");
    }

    private void sendLoop() {
        while (true) {
            try {
                if (sentMessages.isEmpty()) {
                    Thread.sleep(500);
                }
                Thread.sleep(50);

                synchronized (sentMessages) {
                    for (SendMessage msg : sentMessages) {
                        if (!msg.isSend) {
                            taskExecutor.submit(() -> {
                                if (msg.message.getMessageType() == UDPMessage.MessageType.ACK ||
                                        msg.message.getMessageType() == UDPMessage.MessageType.RESPONSE) {
                                    msg.sent();
                                    sendMessage(msg);
                                    sentMessages.removeIf(m -> m.message.getMessageId().equals(msg.message.getMessageId()));
                                } else {
                                    msg.sent();
                                    sendMessage(msg);
                                }
                            });
                        } else if ((System.currentTimeMillis() - msg.timeSent) > ACK_TIMEOUT) {
                            taskExecutor.submit(msg::reSend);
                        }
                    }
                }
            } catch (Exception e) {
                log.logEntry("FE_SendLoop", "Exception", BufferedLog.RequestResponseStatus.FAILURE,
                        e.getMessage(), "Error in send loop");
                e.printStackTrace(System.err);
            }
        }
    }

    private void receiveLoop() {
        byte[] buffer = new byte[4096];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
                UDPMessage msg = new UDPMessage(packet.getData(), packet.getLength());
                Message receivedMsg = new Message(msg, new InetSocketAddress(packet.getAddress(), packet.getPort()));
                receiveMessage.add(receivedMsg);
                log.logEntry("FE_ReceiveLoop", "Message Received", BufferedLog.RequestResponseStatus.SUCCESS,
                        "MessageID: " + msg.getMessageId(), "From: " + packet.getAddress());
                System.out.println("Received message from " + packet.getAddress() + ":" + packet.getPort() + " " + msg);
            } catch (IOException e) {
                log.logEntry("FE_ReceiveLoop", "IOException", BufferedLog.RequestResponseStatus.FAILURE,
                        e.getMessage(), "Error in receiving message");
                System.out.println("IOException in Front-End receiveLoop " + e.getMessage());
            }
        }
    }

    private void processIncomingMessages() {
        while (true) {
            try {
                if (receiveMessage.isEmpty()) {
                    Thread.sleep(500);
                }
                Message msg = receiveMessage.take();
                taskExecutor.submit(() -> handleUDPMessage(msg));
            } catch (InterruptedException e) {
                log.logEntry("FE_ProcessMessages", "InterruptedException", BufferedLog.RequestResponseStatus.FAILURE,
                        e.getMessage(), "Thread interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }

    private void handleAck(String msgId) {
        receiveMessage.removeIf(msg -> Objects.equals(msg.message.getMessageId(), msgId));
        sentMessages.removeIf(msg -> Objects.equals(msg.message.getMessageId(), msgId));
        log.logEntry("FE_HandleAck", "ACK handled", BufferedLog.RequestResponseStatus.SUCCESS,
                "MessageID: " + msgId, "Removed corresponding message from queue");
    }

    private void handleUDPMessage(Message msg) {
        switch (msg.message.getMessageType()) {
            case ACK:
                log.logEntry("FE_HandleMessage", "ACK received", BufferedLog.RequestResponseStatus.SUCCESS,
                        "MessageID: " + msg.message.getMessageId(), "Processing ACK");
                handleAck(msg.message.getMessageId());
                break;
            case RESPONSE:
                log.logEntry("FE_HandleMessage", "Response received", BufferedLog.RequestResponseStatus.SUCCESS,
                        "Sequence: " + msg.message.getSequenceNumber(), "Processing response");
                try {
                    boolean isExist =
                            sequencerQueue.stream().anyMatch(s -> s.sequenceNumber.equals(msg.message.getSequenceNumber()));
                    if (!isExist) {
                        sequencerQueue.add(new ClientRequest(
                                msg.message.getSequenceNumber(),
                                msg.message,
                                new InetSocketAddress(dotenv.get("FE_IP"), Integer.parseInt(dotenv.get("FE_PORT")))));
                    }
                } catch (Exception e) {
                    log.logEntry("FE_HandleMessage", "Error adding to sequencer queue", BufferedLog.RequestResponseStatus.FAILURE,
                            e.getMessage(), "");
                    System.out.println(e.getMessage());
                }
                break;
            case RESULT:
                log.logEntry("FE_HandleMessage", "Result message received", BufferedLog.RequestResponseStatus.SUCCESS,
                        "Sequence: " + msg.message.getSequenceNumber(), "Processing result");
                System.out.println("Received Result: " + msg.message.getSequenceNumber());
                msg.message.setMessageType(UDPMessage.MessageType.ACK);
                sentMessages.add(new SendMessage(msg));
                synchronized (sequencerQueue) {
                    ClientRequest cr = sequencerQueue.stream().peek(req -> req.sequenceNumber.equals(msg.message.getSequenceNumber())).findFirst().orElse(null);
                    if (cr != null) {
                        if (msg.message.getPayload() instanceof String str) {
                            cr.addResponse(msg.endpoint.getAddress(), str);
                            log.logEntry("FE_HandleMessage", "Response added to client request", BufferedLog.RequestResponseStatus.SUCCESS,
                                    "Response: " + str, "Sequence: " + cr.sequenceNumber);
                        }
                    } else {
                        ClientRequest ncr = new ClientRequest(
                                msg.message.getSequenceNumber(),
                                msg.message,
                                new InetSocketAddress(dotenv.get("FE_IP"), Integer.parseInt(dotenv.get("FE_PORT"))));
                        String str = (String) msg.message.getPayload();
                        ncr.addResponse(msg.endpoint.getAddress(), str);
                        ClientRequest reCr = sequencerQueue.stream().peek(req -> req.sequenceNumber.equals(msg.message.getSequenceNumber())).findFirst().orElse(null);
                        if (reCr != null) {
                            reCr.addResponse(msg.endpoint.getAddress(), str);
                        } else {
                            sequencerQueue.add(ncr);
                        }
                        log.logEntry("FE_HandleMessage", "Response added to client request", BufferedLog.RequestResponseStatus.SUCCESS,
                                "Response: " + str, "Sequence: " + cr.sequenceNumber);
                    }
                }
                break;
            case REQUEST:
                log.logEntry("FE_HandleMessage", "Request message received", BufferedLog.RequestResponseStatus.INFO,
                        "Processing new request", "Forwarding ACK and sending to Sequencer");
                try {
                    msg.message.addEndpoint(InetAddress.getByName(dotenv.get("FE_IP")), Integer.parseInt(dotenv.get("FE_PORT")));
                    msg.message.setMessageType(UDPMessage.MessageType.ACK);
                    sentMessages.add(new SendMessage(msg));

                    UDPMessage rmMessage = new UDPMessage(UDPMessage.MessageType.REQUEST, msg.message.getAction(), 0, msg.message.getEndpoints(), msg.message.getPayload());
                    sentMessages.add(new SendMessage(rmMessage, new InetSocketAddress(InetAddress.getByName(dotenv.get("SEQUENCER_IP")), Integer.parseInt(dotenv.get("SEQUENCER_PORT")))));
                } catch (Exception e) {
                    log.logEntry("FE_HandleMessage", "Exception handling REQUEST", BufferedLog.RequestResponseStatus.FAILURE,
                            e.getMessage(), "");
                    System.out.println("Exception in Front-End handleUDPMessage " + e.getMessage());
                }
                break;
            default:
                log.logEntry("FE_HandleMessage", "Unknown message type", BufferedLog.RequestResponseStatus.DEBUG,
                        msg.message.toString(), "Received unknown message");
                System.out.println("Received unknown message type: " + msg);
        }
    }

    private void checkResultFound() {
        while (true) {
            try {
                if (sequencerQueue.isEmpty()) {
                    Thread.sleep(500);
                }
                synchronized (sequencerQueue) {
                    for (ClientRequest cr : sequencerQueue) {
                        if ((System.currentTimeMillis() - cr.timestamp) < REPLICA_RESPONSE_TIMEOUT && !cr.isTimeOut()) {
                            taskExecutor.submit(() -> {
                                ReplicateResponseMetric metric = cr.analyzeResponses();
                                if (metric.matchedInetAddress.size() >= 3 && cr.isSendToClient) {
                                    sequencerQueue.removeIf(c -> Objects.equals(c.sequenceNumber, cr.sequenceNumber));
                                    log.logEntry("FE_CheckResults", "Majority achieved", BufferedLog.RequestResponseStatus.SUCCESS,
                                            metric.majorityResult(), "Request " + cr.sequenceNumber + " completed");
                                } else if (metric.matchedInetAddress.size() >= 2 && !cr.isSendToClient) {
                                    cr.sendToClient();
                                    boolean isAlreadyAdded =
                                            sentMessages.stream().anyMatch(s -> s.message.getSequenceNumber() == cr.sequenceNumber && s.message.getMessageType() == UDPMessage.MessageType.RESPONSE);
                                    if (!isAlreadyAdded) {
                                        UDPMessage msg = new UDPMessage(UDPMessage.MessageType.RESPONSE, cr.originalMessage.getAction(), 0, cr.originalMessage.getEndpoints(), cr.originalMessage.getSequenceNumber(), metric.majorityResult);
                                        sentMessages.add(new SendMessage(msg, cr.clientInetSocketAddress));
                                        log.logEntry("FE_CheckResults", "Aggregated responses", BufferedLog.RequestResponseStatus.SUCCESS,
                                                metric.majorityResult(), "Final response prepared for request " + "sequenceNumber " + cr.sequenceNumber);
                                    }
                                } else if (!metric.errorInetAddress.isEmpty() && cr.isSendToClient) {
                                    cr.sendToServer();
                                    boolean isAlreadyAdded =
                                            sentMessages.stream().anyMatch(s -> s.message.getSequenceNumber() == cr.sequenceNumber && s.message.getMessageType() == UDPMessage.MessageType.INCORRECT_RESULT_NOTIFICATION);
                                    if (!isAlreadyAdded) {
                                        StringBuilder errorMsg = new StringBuilder("Incorrect Message::");
                                        for (InetAddress addr : metric.errorInetAddress) {
                                            errorMsg.append(addr.getHostAddress()).append("::");
                                        }
                                        int length = errorMsg.length();
                                        if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                            errorMsg.setLength(length - 2);
                                        }
                                        UDPMessage msg = new UDPMessage(UDPMessage.MessageType.INCORRECT_RESULT_NOTIFICATION, cr.originalMessage.getAction(), 0, cr.originalMessage.getEndpoints(), cr.originalMessage.getSequenceNumber(), errorMsg.toString());
                                        for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                            sentMessages.add(new SendMessage(msg, rmEndpoint));
                                        }
                                        log.logEntry("FE_CheckResults", "Mismatch detected", BufferedLog.RequestResponseStatus.FAILURE,
                                                errorMsg.toString(), "Notified RM for request " + cr.sequenceNumber);
                                    }
                                }
                            });
                        } else if (!cr.isTimeOut() && !cr.isFull()) {
                            cr.timeOut();
                            boolean isAlreadyAdded =
                                    sentMessages.stream().anyMatch(s -> s.message.getSequenceNumber() == cr.sequenceNumber && s.message.getMessageType() == UDPMessage.MessageType.CRASH_NOTIFICATION);
                            if (!isAlreadyAdded) {
                                UDPMessage msg = new UDPMessage(UDPMessage.MessageType.CRASH_NOTIFICATION, cr.originalMessage.getAction(), 0, cr.originalMessage.getEndpoints(), cr.originalMessage.getSequenceNumber(), cr.originalMessage.getPayload());
                                for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                    sentMessages.add(new SendMessage(msg, rmEndpoint));
                                }
                                log.logEntry("FE_CheckResults", "Timeout detected", BufferedLog.RequestResponseStatus.FAILURE,
                                        "No response", "Crash notification sent for request " + cr.sequenceNumber);
                            }
                        } else {
                            cr.timeOut();
                            boolean isAlreadyAdded =
                                    sentMessages.stream().anyMatch(s -> s.message.getSequenceNumber() == cr.sequenceNumber && s.message.getMessageType() == UDPMessage.MessageType.RESULT_TIMEOUT);
                            if (!isAlreadyAdded) {
                                UDPMessage msg = new UDPMessage(UDPMessage.MessageType.RESULT_TIMEOUT, cr.originalMessage.getAction(), 0, cr.originalMessage.getEndpoints(), cr.originalMessage.getSequenceNumber(), cr.originalMessage.getPayload());
                                for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                    sentMessages.add(new SendMessage(msg, rmEndpoint));
                                }
                                log.logEntry("FE_CheckResults", "Result timeout", BufferedLog.RequestResponseStatus.FAILURE,
                                        "Timeout reached", "Timeout notification sent for request " + cr.sequenceNumber);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.logEntry("FE_CheckResults", "Exception", BufferedLog.RequestResponseStatus.FAILURE,
                        e.getMessage(), "Error while checking results");
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Shutdown] Cleaning up resources...");
            taskExecutor.shutdown();
            try {
                if (!taskExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    taskExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                taskExecutor.shutdownNow();
            }
            if (log != null) {
                log.shutdown();
            }
            System.out.println("[Shutdown] Cleanup complete.");
        }));
        List<InetSocketAddress> rmEndpoints = new ArrayList<>();
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_ONE_IP")), Integer.parseInt(dotenv.get("RM_ONE_PORT"))));
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_TWO_IP")), Integer.parseInt(dotenv.get("RM_TWO_PORT"))));
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_THREE_IP")), Integer.parseInt(dotenv.get("RM_THREE_PORT"))));

        FrontEnd fe = new FrontEnd(rmEndpoints);
        fe.start();

        System.out.println("[Front-End] Front-End started.");
        log.logEntry("FE_Main", "Startup complete", BufferedLog.RequestResponseStatus.SUCCESS,
                "Front-End service launched", "Ready to process client requests");
    }
}
