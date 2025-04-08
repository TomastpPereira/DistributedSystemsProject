import io.github.cdimascio.dotenv.Dotenv;
import network.UDPMessage;
import utility.BufferedLog;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class FrontEnd {
    private InetSocketAddress sequencerEndpoint;
    private List<InetSocketAddress> replicaManagerEndpoints;
    private DatagramSocket socket;
    private BlockingQueue<UDPMessage> incomingQueue = new LinkedBlockingQueue<>();
    private ExecutorService taskExecutor = Executors.newFixedThreadPool(5);
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ConcurrentHashMap<String, SentMessage> sentMessages = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, SequenceTask> sequenceTasks = new ConcurrentHashMap<>();
    private static BufferedLog log;

    private static int fePort;

    private static long ACK_TIMEOUT;
    private static long SEQUENCER_TIMEOUT;
    private static long REPLICA_RESPONSE_TIMEOUT;

    public FrontEnd(InetSocketAddress sequencerEndpoint, List<InetSocketAddress> replicaManagerEndpoints) throws SocketException {
        this.sequencerEndpoint = sequencerEndpoint;
        this.replicaManagerEndpoints = replicaManagerEndpoints;
        this.socket = new DatagramSocket(fePort);
        fePort = Integer.parseInt(Dotenv.load().get("FE_PORT"));
        ACK_TIMEOUT = Long.parseLong(Dotenv.load().get("ACK_TIMEOUT"));
        SEQUENCER_TIMEOUT = Long.parseLong(Dotenv.load().get("SEQUENCE_TIMEOUT"));
        REPLICA_RESPONSE_TIMEOUT = Long.parseLong(Dotenv.load().get("REPLICA_RESPONSE_TIMEOUT"));
        log = new BufferedLog("logs/", "Front-End", "Front-End");
    }

    private static class SentMessage {
        UDPMessage message;
        long timeSent;

        public SentMessage(UDPMessage message, long timeSent) {
            this.message = message;
            this.timeSent = timeSent;
        }
    }

    private static class SequenceTask {
        UDPMessage originalRequest;
        volatile boolean sequencerAckReceived = false;
        List<UDPMessage> replicaResponses = Collections.synchronizedList(new ArrayList<>());
        long timeSent;

        public SequenceTask(UDPMessage originalRequest, long timeSent) {
            this.originalRequest = originalRequest;
            this.timeSent = timeSent;
        }
    }

    public void start() {
        new Thread(this::receiveLoop).start();
        new Thread(this::processIncomingMessages).start();
        scheduler.scheduleAtFixedRate(this::checkAndResendMessages, ACK_TIMEOUT, ACK_TIMEOUT, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::checkSequenceTasks, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private void receiveLoop() {
        byte[] buffer = new byte[4096];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
                UDPMessage msg = deserialize(packet.getData(), packet.getLength());
                incomingQueue.offer(msg);
            } catch (IOException e) {
                System.out.println("IOException in Front-End receiveLoop " + e.getMessage());
            }
        }
    }

    private void processIncomingMessages() {
        while (true) {
            try {
                UDPMessage msg = incomingQueue.take();
                taskExecutor.submit(() -> handleUDPMessage(msg));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void handleUDPMessage(UDPMessage msg) {
        switch (msg.getMessageType()) {
            case ACK:
                handleAck(msg);
                break;
            case RESPONSE:
                handleReplicaResponse(msg);
                break;
            default:
                System.out.println("Received unknown message type: " + msg);
        }
    }

    private void handleAck(UDPMessage ackMsg) {
        String msgId = ackMsg.getMessageId();
        if (sentMessages.containsKey(msgId)) {
            sentMessages.remove(msgId);
            SequenceTask task = sequenceTasks.get(msgId);
            if (task != null) {
                task.sequencerAckReceived = true;
                System.out.println("Sequencer ACK received for message: " + msgId);
            }
        }
    }

    private void handleReplicaResponse(UDPMessage respMsg) {
        String msgId = respMsg.getMessageId();
        SequenceTask task = sequenceTasks.get(msgId);
        if (task != null) {
            task.replicaResponses.add(respMsg);
            System.out.println("Received response from replica: " + respMsg.getEndpoints() + " for message: " + msgId);
            if (task.replicaResponses.size() >= 3) {
                String correctResult = validateReplicaResponses(task.replicaResponses);
                if (correctResult != null) {
                    sendResultToClient(task.originalRequest, correctResult);
                    sequenceTasks.remove(msgId);
                } else {
                    long now = System.currentTimeMillis();
                    if (now - task.timeSent > REPLICA_RESPONSE_TIMEOUT) {
                        reportReplicaFailure(msgId, "Inconsistent responses from replicas");
                        sequenceTasks.remove(msgId);
                    }
                }
            }
        } else {
            System.out.println("No sequence task found for response: " + msgId);
        }
    }

    private String validateReplicaResponses(List<UDPMessage> responses) {
        Map<String, Integer> count = new HashMap<>();
        for (UDPMessage resp : responses) {
            String result = (String) resp.getPayload();
            count.put(result, count.getOrDefault(result, 0) + 1);
        }
        for (Map.Entry<String, Integer> entry : count.entrySet()) {
            if (entry.getValue() >= 2) {
                return entry.getKey();
            }
        }
        return null;
    }

    private void sendResultToClient(UDPMessage originalRequest, String result) {
        Map<InetAddress, Integer> endpoints = originalRequest.getEndpoints();
        InetAddress clientAddr = endpoints.keySet().iterator().next();
        int clientPort = endpoints.get(clientAddr);
        UDPMessage msg = new UDPMessage(UDPMessage.MessageType.RESPONSE,
                originalRequest.getAction(), 0, endpoints, result);
        sendUDPMessage(msg, clientAddr, clientPort);
        System.out.println("Sent final result to client: " + result);
    }

    private void checkAndResendMessages() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, SentMessage> entry : sentMessages.entrySet()) {
            SentMessage sm = entry.getValue();
            if (now - sm.timeSent >= ACK_TIMEOUT) {
                sm.message.setRetry(sm.message.getRetry() + 1);
                sendUDPMessage(sm.message, sequencerEndpoint.getAddress(), sequencerEndpoint.getPort());
                sm.timeSent = now;
                System.out.println("Resent message: " + sm.message.getMessageId() + " Retry: " + sm.message.getRetry());
            }
        }
    }

    private void checkSequenceTasks() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, SequenceTask> entry : sequenceTasks.entrySet()) {
            SequenceTask task = entry.getValue();
            if (!task.sequencerAckReceived && now - task.timeSent >= SEQUENCER_TIMEOUT) {
                reportSequencerFailure(task.originalRequest.getMessageId());
                task.timeSent = now;
            }
            if (now - task.timeSent >= REPLICA_RESPONSE_TIMEOUT && task.replicaResponses.size() < 3) {
                reportReplicaFailure(task.originalRequest.getMessageId(), "Incomplete replica responses");
                sequenceTasks.remove(entry.getKey());
            }
        }
    }

    private void reportReplicaFailure(String messageId, String reason) {
        String report = "Replica failure for message: " + messageId + " Reason: " + reason;
        UDPMessage failMsg = new UDPMessage(UDPMessage.MessageType.FAILURE_NOTIFICATION, messageId,
                "ReplicaFailure", 0, getReplicaManagerEndpointsMap(), report);
        for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
            sendUDPMessage(failMsg, rmEndpoint.getAddress(), rmEndpoint.getPort());
        }
        System.out.println("Reported replica failure for message: " + messageId);
    }

    private Map<InetAddress, Integer> getReplicaManagerEndpointsMap() {
        Map<InetAddress, Integer> map = new HashMap<>();
        for (InetSocketAddress addr : replicaManagerEndpoints) {
            map.put(addr.getAddress(), addr.getPort());
        }
        return map;
    }

    private void sendUDPMessage(UDPMessage msg, InetAddress destAddress, int destPort) {
        try {
            byte[] data = serialize(msg);
            DatagramPacket packet = new DatagramPacket(data, data.length, destAddress, destPort);
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] serialize(UDPMessage msg) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(msg);
        oos.flush();
        return baos.toByteArray();
    }

    private UDPMessage deserialize(byte[] data, int length) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data, 0, length);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (UDPMessage) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Class not found during deserialization", e);
        }
    }

    public void sendClientRequestToSequencer(UDPMessage clientRequest) {
        sequenceTasks.put(clientRequest.getMessageId(), new SequenceTask(clientRequest, System.currentTimeMillis()));
        UDPMessage sequencerMsg = new UDPMessage(UDPMessage.MessageType.REQUEST, clientRequest.getMessageId(),
                clientRequest.getAction(), 0, clientRequest.getEndpoints(), clientRequest.getPayload());
        sendUDPMessage(sequencerMsg, sequencerEndpoint.getAddress(), sequencerEndpoint.getPort());
        sentMessages.put(sequencerMsg.getMessageId(), new SentMessage(sequencerMsg, System.currentTimeMillis()));
        System.out.println("Sent client request to Sequencer: " + sequencerMsg);
    }

    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Shutdown] Cleaning up resources...");
            if (log != null) {
                log.log("Program is shutting down...");
                log.shutdown();
            }
            System.out.println("[Shutdown] Cleanup complete.");
        }));

        InetSocketAddress sequencerEndpoint = new InetSocketAddress(InetAddress.getByName(Dotenv.load().get("SEQUENCER_PORT")), Integer.parseInt(Dotenv.load().get("SEQUENCER_PORT")));

        List<InetSocketAddress> rmEndpoints = new ArrayList<>();
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(Dotenv.load().get("RM_ONE_IP")), Integer.parseInt(Dotenv.load().get("RM_ONE_PORT"))));
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(Dotenv.load().get("RM_TWO_IP")), Integer.parseInt(Dotenv.load().get("RM_TWO_PORT"))));
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(Dotenv.load().get("RM_THREE_PORT")), Integer.parseInt(Dotenv.load().get("RM_THREE_PORT"))));

        FrontEnd fe = new FrontEnd(sequencerEndpoint, rmEndpoints);
        fe.start();

//        Map<InetAddress, Integer> clientEndpoint = new HashMap<>();
//        clientEndpoint.put(InetAddress.getByName(Dotenv.load().get("SEQUENCER_PORT")), 8000);
//        UDPMessage clientReq = new UDPMessage(UDPMessage.MessageType.REQUEST,
//                "purchaseShare", 0, clientEndpoint, "Buy 5 shares of NYKM100925");
//        fe.sendClientRequestToSequencer(clientReq);
    }
}
