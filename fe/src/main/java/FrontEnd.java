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
            .directory(Paths.get(System.getProperty("user.dir")).toString()) //.getParent()
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
    }

    private class Message {
        final UDPMessage message;
        final long timeSent;
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
        }

        public void addResponse(InetAddress address, String response) {
            if (!isFull()) {
                if (replicaResponses.containsKey(address)) {
                    replicaResponses.replace(address, response);
                } else {
                    replicaResponses.put(address, response);
                }
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

                groupedByResult
                        .computeIfAbsent(result, k -> new ArrayList<>())
                        .add(address);
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
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    public void start() {
        new Thread(this::receiveLoop).start();
        new Thread(this::sendLoop).start();
        new Thread(this::processIncomingMessages).start();
        new Thread(this::checkResultFound).start();
    }

    private void receiveLoop() {
        byte[] buffer = new byte[4096];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
                UDPMessage msg = new UDPMessage(packet.getData(), packet.getLength());
                receiveMessage.add(new Message(msg, new InetSocketAddress(packet.getAddress(), packet.getPort())));
            } catch (IOException e) {
                System.out.println("IOException in Front-End receiveLoop " + e.getMessage());
            }
        }
    }

    private void sendLoop() {
        while (true) {
            try {
                if (sentMessages.isEmpty()) {
                    Thread.sleep(500);
                }
                for (SendMessage msg : sentMessages) {
                    if (!msg.isSend) {
                        taskExecutor.submit(() -> {
                            if (msg.message.getMessageType() == UDPMessage.MessageType.ACK) {
                                sendMessage(msg);
                                sentMessages.removeIf(m -> m.message.getMessageId().equals(msg.message.getMessageId()));
                            } else {
                                sendMessage(msg);
                                msg.sent();
                            }
                        });
                    } else if ((System.currentTimeMillis() - msg.timeSent) > ACK_TIMEOUT) {
                        taskExecutor.submit(msg::reSend);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
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
                Thread.currentThread().interrupt();
            }
        }
    }

    private void checkResultFound() {
        while (true) {
            try {
                if (sequencerQueue.isEmpty()) {
                    Thread.sleep(500);
                }
                for (ClientRequest cr : sequencerQueue) {
                    if ((System.currentTimeMillis() - cr.timestamp) < REPLICA_RESPONSE_TIMEOUT && !cr.isTimeOut()) {
                        taskExecutor.submit(() -> {
                            ReplicateResponseMetric metric = cr.analyzeResponses();
                            if (metric.matchedInetAddress.size() >= 3 && cr.isSendToClient) {
                                sequencerQueue.removeIf(c -> Objects.equals(c.sequenceNumber, cr.sequenceNumber));
                            } else if (metric.matchedInetAddress.size() >= 2 && !cr.isSendToClient) {
                                cr.sendToClient();
                                UDPMessage msg = new UDPMessage(UDPMessage.MessageType.RESPONSE, cr.originalMessage.getAction(), 0, cr.originalMessage.getEndpoints(), cr.originalMessage.getSequenceNumber(), cr.originalMessage.getPayload());
                                sentMessages.add(new SendMessage(msg, cr.clientInetSocketAddress));
                            } else if (!metric.errorInetAddress.isEmpty() && cr.isSendToClient) {
                                cr.sendToServer();
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
                            }
                        });
                    } else if (!cr.isTimeOut() && !cr.isFull()) {
                        cr.timeOut();
                        UDPMessage msg = new UDPMessage(UDPMessage.MessageType.CRASH_NOTIFICATION, cr.originalMessage.getAction(), 0, cr.originalMessage.getEndpoints(), cr.originalMessage.getSequenceNumber(), cr.originalMessage.getPayload());
                        for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                            sentMessages.add(new SendMessage(msg, rmEndpoint));
                        }
                    } else {
                        cr.timeOut();
                        UDPMessage msg = new UDPMessage(UDPMessage.MessageType.RESULT_TIMEOUT, cr.originalMessage.getAction(), 0, cr.originalMessage.getEndpoints(), cr.originalMessage.getSequenceNumber(), cr.originalMessage.getPayload());
                        for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                            sentMessages.add(new SendMessage(msg, rmEndpoint));
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void handleAck(String msgId) {
        receiveMessage.removeIf(msg -> Objects.equals(msg.message.getMessageId(), msgId));
    }

    private void handleUDPMessage(Message msg) {
        switch (msg.message.getMessageType()) {
            case ACK:
                handleAck(msg.message.getMessageId());
                break;
            case RESPONSE:
                try {
                    sequencerQueue.add(new ClientRequest(msg.message.getSequenceNumber(), msg.message, new InetSocketAddress(dotenv.get("FE_IP"), msg.message.getEndpoints().get(InetAddress.getByName(dotenv.get("FE_SERVICE_IP"))))));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
                break;
            case RESULT:
                msg.message.setMessageType(UDPMessage.MessageType.ACK);
                sentMessages.add(new SendMessage(msg));
                ClientRequest cr = sequencerQueue.stream().filter(req -> req.sequenceNumber == msg.message.getSequenceNumber()).findFirst().orElse(null);
                if (cr != null) {
                    if (msg.message.getPayload() instanceof String str) {
                        cr.addResponse(msg.endpoint.getAddress(), str);
                    }
                } else {
                    System.out.println("Sequencer number not found! Issue with sequencer");
                }
                break;
            case REQUEST:
                try {
                    msg.message.addEndpoint(InetAddress.getByName(dotenv.get("FE_IP")), Integer.parseInt(dotenv.get("FE_PORT")));
                    msg.message.setMessageType(UDPMessage.MessageType.ACK);
                    sentMessages.add(new SendMessage(msg));

                    UDPMessage rmMessage = new UDPMessage(UDPMessage.MessageType.REQUEST, msg.message.getAction(), 0, msg.message.getEndpoints(), msg.message.getPayload());
                    sentMessages.add(new SendMessage(rmMessage, new InetSocketAddress(InetAddress.getByName(dotenv.get("SEQUENCER_IP")), Integer.parseInt(dotenv.get("SEQUENCER_PORT")))));
                } catch (Exception e) {
                    System.out.println("Exception in Front-End handleUDPMessage " + e.getMessage());
                }
                break;
            default:
                System.out.println("Received unknown message type: " + msg);
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
    }
}