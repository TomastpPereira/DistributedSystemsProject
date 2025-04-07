import network.UDPMessage;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

// Assumed network.UDPMessage class from earlier (without sequenceNumber, with messageId, action, retry, and endpoints).
// import your.network.UDPMessage;

public class FrontEnd {

    // FE configuration: Known sequencer and replica manager endpoints.
    private InetSocketAddress sequencerEndpoint;
    private List<InetSocketAddress> replicaManagerEndpoints;

    // UDP socket for sending and receiving messages.
    private DatagramSocket socket;

    // BlockingQueue for incoming UDP messages.
    private BlockingQueue<UDPMessage> incomingQueue = new LinkedBlockingQueue<>();

    // Executor for processing tasks from the message queue.
    private ExecutorService taskExecutor = Executors.newFixedThreadPool(10);

    // Scheduled executor for periodic timeout/retry tasks.
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // Map to track sent messages awaiting ACK (keyed by messageId).
    private ConcurrentHashMap<String, SentMessage> sentMessages = new ConcurrentHashMap<>();

    // Map to track sequence tasks (client requests sent to Sequencer) awaiting sequencer response and replica responses.
    private ConcurrentHashMap<String, SequenceTask> sequenceTasks = new ConcurrentHashMap<>();

    // FE’s port (the UDP socket is bound to this port)
    private int fePort = 5000;

    // Timeout values (in milliseconds)
    private static final long ACK_TIMEOUT = 5000;        // 5 seconds for ACK
    private static final long SEQUENCER_TIMEOUT = 5000;    // 5 seconds for sequencer response
    private static final long REPLICA_RESPONSE_TIMEOUT = 8000; // 8 seconds for replica responses

    // Constructor: initialize known endpoints and start the FE socket.
    public FrontEnd(InetSocketAddress sequencerEndpoint, List<InetSocketAddress> replicaManagerEndpoints) throws SocketException {
        this.sequencerEndpoint = sequencerEndpoint;
        this.replicaManagerEndpoints = replicaManagerEndpoints;
        this.socket = new DatagramSocket(fePort);
    }

    // Inner class to represent a sent message that awaits an ACK.
    private static class SentMessage {
        UDPMessage message;
        long timeSent;  // Timestamp in millis

        public SentMessage(UDPMessage message, long timeSent) {
            this.message = message;
            this.timeSent = timeSent;
        }
    }

    // Inner class to represent a sequence task for a client request.
    private static class SequenceTask {
        UDPMessage originalRequest;  // The client request forwarded to sequencer.
        volatile boolean sequencerAckReceived = false;
        // List of replica responses received (for simplicity, using a synchronized list)
        List<UDPMessage> replicaResponses = Collections.synchronizedList(new ArrayList<>());
        long timeSent;  // Time when task was sent to sequencer.

        public SequenceTask(UDPMessage originalRequest, long timeSent) {
            this.originalRequest = originalRequest;
            this.timeSent = timeSent;
        }
    }

    // Main FE loop: listens for incoming UDP messages and enqueues them.
    public void start() {
        // Start receiver thread.
        new Thread(() -> receiveLoop()).start();

        // Start a thread to process the incoming message queue.
        new Thread(() -> processIncomingMessages()).start();

        // Schedule periodic task for retrying unacknowledged messages.
        scheduler.scheduleAtFixedRate(() -> checkAndResendMessages(), ACK_TIMEOUT, ACK_TIMEOUT, TimeUnit.MILLISECONDS);

        // Schedule periodic task for checking sequence tasks (sequencer and replica responses).
        scheduler.scheduleAtFixedRate(() -> checkSequenceTasks(), 1000, 1000, TimeUnit.MILLISECONDS);

        System.out.println("Front End started on port " + fePort);
    }

    // Loop to receive UDP messages.
    private void receiveLoop() {
        byte[] buffer = new byte[4096];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
                // Deserialize the network.UDPMessage from packet.
                UDPMessage msg = deserialize(packet.getData(), packet.getLength());
                // Enqueue the message for processing.
                incomingQueue.offer(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Process messages from the incomingQueue using a thread pool.
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

    // Main message handler.
    private void handleUDPMessage(UDPMessage msg) {
        // Based on message type, perform tasks.
        switch (msg.getMessageType()) {
            case ACK:
                handleAck(msg);
                break;
            case RESPONSE:
                handleReplicaResponse(msg);
                break;
            case FAILURE_NOTIFICATION:
                // FE could log or forward these to client if needed.
                System.out.println("Received FAILURE_NOTIFICATION: " + msg);
                break;
            // You may add other cases like VOTE, PING if needed.
            default:
                System.out.println("Received unknown message type: " + msg);
        }
    }

    // When an ACK is received, remove the corresponding message from sentMessages.
    private void handleAck(UDPMessage ackMsg) {
        String msgId = ackMsg.getMessageId();
        if (sentMessages.containsKey(msgId)) {
            sentMessages.remove(msgId);
            // Also, if this ACK is from the sequencer for a request, mark the sequence task.
            if (ackMsg.getAction().equalsIgnoreCase("SequencerACK")) {
                SequenceTask task = sequenceTasks.get(msgId);
                if (task != null) {
                    task.sequencerAckReceived = true;
                    System.out.println("Sequencer ACK received for message: " + msgId);
                }
            }
        }
    }

    // When a replica response is received.
    private void handleReplicaResponse(UDPMessage respMsg) {
        // The messageId ties the response to the original client request.
        String msgId = respMsg.getMessageId();
        SequenceTask task = sequenceTasks.get(msgId);
        if (task != null) {
            task.replicaResponses.add(respMsg);
            System.out.println("Received response from replica: " + respMsg.getEndpoints() + " for message: " + msgId);
            // Check if we have all three responses.
            if (task.replicaResponses.size() >= 3) {
                // Validate responses (e.g., check if at least 2 match).
                String correctResult = validateReplicaResponses(task.replicaResponses);
                if (correctResult != null) {
                    // Send the correct result to the client.
                    sendResultToClient(task.originalRequest, correctResult);
                    // Remove the task.
                    sequenceTasks.remove(msgId);
                } else {
                    // If responses are not consistent and 8 seconds have passed, report failure.
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

    // Validate replica responses; returns the correct result if two responses match, otherwise null.
    private String validateReplicaResponses(List<UDPMessage> responses) {
        Map<String, Integer> count = new HashMap<>();
        for (UDPMessage resp : responses) {
            String result = (String) resp.getPayload();
            count.put(result, count.getOrDefault(result, 0) + 1);
        }
        // Find if any result appears at least twice.
        for (Map.Entry<String, Integer> entry : count.entrySet()) {
            if (entry.getValue() >= 2) {
                return entry.getKey();
            }
        }
        return null;
    }

    // Send the final result to the client using the first endpoint in the original request.
    private void sendResultToClient(UDPMessage originalRequest, String result) {
        // Assume originalRequest.endpoints contains at least one client endpoint.
        Map<InetAddress, Integer> endpoints = originalRequest.getEndpoints();
        InetAddress clientAddr = endpoints.keySet().iterator().next();
        int clientPort = endpoints.get(clientAddr);
        UDPMessage msg = new UDPMessage(UDPMessage.MessageType.RESPONSE, originalRequest.getMessageId(),
                originalRequest.getAction(), 0, endpoints, result);
        sendUDPMessage(msg, clientAddr, clientPort);
        System.out.println("Sent final result to client: " + result);
    }

    // Periodic task to check sentMessages for unacknowledged messages and resend them.
    private void checkAndResendMessages() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, SentMessage> entry : sentMessages.entrySet()) {
            SentMessage sm = entry.getValue();
            if (now - sm.timeSent >= ACK_TIMEOUT) {
                // Increment retry counter and resend.
                sm.message.setRetry(sm.message.getRetry() + 1);
                // Resend message (we need to know destination from the endpoints map).
                // For simplicity, assume we are resending to the sequencer if it's a REQUEST.
                sendUDPMessage(sm.message, sequencerEndpoint.getAddress(), sequencerEndpoint.getPort());
                // Update timeSent.
                sm.timeSent = now;
                System.out.println("Resent message: " + sm.message.getMessageId() + " Retry: " + sm.message.getRetry());
            }
        }
    }

    // Periodic task to check sequence tasks for missing sequencer response or replica responses.
    private void checkSequenceTasks() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, SequenceTask> entry : sequenceTasks.entrySet()) {
            SequenceTask task = entry.getValue();
            // Check if sequencer ACK was not received within 5 seconds.
            if (!task.sequencerAckReceived && now - task.timeSent >= SEQUENCER_TIMEOUT) {
                // Report missing sequencer response to all replica managers.
                reportSequencerFailure(task.originalRequest.getMessageId());
                // Optionally, remove task or update timestamp.
                task.timeSent = now;
            }
            // Check if replica responses are missing after 8 seconds.
            if (now - task.timeSent >= REPLICA_RESPONSE_TIMEOUT && task.replicaResponses.size() < 3) {
                reportReplicaFailure(task.originalRequest.getMessageId(), "Incomplete replica responses");
                // Optionally, remove task.
                sequenceTasks.remove(entry.getKey());
            }
        }
    }

    // Report a sequencer failure to all replica managers.
    private void reportSequencerFailure(String messageId) {
        String report = "Sequencer timeout for message: " + messageId;
        UDPMessage failMsg = new UDPMessage(UDPMessage.MessageType.FAILURE_NOTIFICATION, messageId,
                "SequencerFailure", 0, getReplicaManagerEndpointsMap(), report);
        for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
            sendUDPMessage(failMsg, rmEndpoint.getAddress(), rmEndpoint.getPort());
        }
        System.out.println("Reported sequencer failure for message: " + messageId);
    }

    // Report a replica failure to all replica managers.
    private void reportReplicaFailure(String messageId, String reason) {
        String report = "Replica failure for message: " + messageId + " Reason: " + reason;
        UDPMessage failMsg = new UDPMessage(UDPMessage.MessageType.FAILURE_NOTIFICATION, messageId,
                "ReplicaFailure", 0, getReplicaManagerEndpointsMap(), report);
        for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
            sendUDPMessage(failMsg, rmEndpoint.getAddress(), rmEndpoint.getPort());
        }
        System.out.println("Reported replica failure for message: " + messageId);
    }

    // Helper to create a Map of endpoints for reporting purposes (could be a combined map).
    private Map<InetAddress, Integer> getReplicaManagerEndpointsMap() {
        Map<InetAddress, Integer> map = new HashMap<>();
        for (InetSocketAddress addr : replicaManagerEndpoints) {
            map.put(addr.getAddress(), addr.getPort());
        }
        return map;
    }

    // Method to send a network.UDPMessage to a specified destination.
    private void sendUDPMessage(UDPMessage msg, InetAddress destAddress, int destPort) {
        try {
            byte[] data = serialize(msg);
            DatagramPacket packet = new DatagramPacket(data, data.length, destAddress, destPort);
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Serialize network.UDPMessage to byte array.
    private byte[] serialize(UDPMessage msg) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(msg);
        oos.flush();
        return baos.toByteArray();
    }

    // Deserialize network.UDPMessage from byte array.
    private UDPMessage deserialize(byte[] data, int length) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data, 0, length);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (UDPMessage) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Class not found during deserialization", e);
        }
    }

    // Method to send a client request to the Sequencer.
    // This method is called when a client request is received.
    public void sendClientRequestToSequencer(UDPMessage clientRequest) {
        // Record the original request in the sequenceTasks map.
        sequenceTasks.put(clientRequest.getMessageId(), new SequenceTask(clientRequest, System.currentTimeMillis()));
        // Create a network.UDPMessage for the sequencer; action remains same.
        UDPMessage sequencerMsg = new UDPMessage(UDPMessage.MessageType.REQUEST, clientRequest.getMessageId(),
                clientRequest.getAction(), 0, clientRequest.getEndpoints(), clientRequest.getPayload());
        // Send the message to the Sequencer.
        sendUDPMessage(sequencerMsg, sequencerEndpoint.getAddress(), sequencerEndpoint.getPort());
        // Add to sentMessages for ACK tracking.
        sentMessages.put(sequencerMsg.getMessageId(), new SentMessage(sequencerMsg, System.currentTimeMillis()));
        System.out.println("Sent client request to Sequencer: " + sequencerMsg);
    }

    // Main method for FE.
    public static void main(String[] args) throws Exception {
        // Example configuration: replace with actual IPs/ports.
        InetAddress sequencerIP = InetAddress.getByName("127.0.0.1");
        InetSocketAddress sequencerEndpoint = new InetSocketAddress(sequencerIP, 6000);

        List<InetSocketAddress> rmEndpoints = new ArrayList<>();
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 7001));
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 7002));
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 7003));

        FrontEnd fe = new FrontEnd(sequencerEndpoint, rmEndpoints);
        fe.start();

        // For demonstration, simulate a client request.
        // Construct a dummy client request network.UDPMessage.
        Map<InetAddress, Integer> clientEndpoint = new HashMap<>();
        clientEndpoint.put(InetAddress.getByName("127.0.0.1"), 8000);
        UDPMessage clientReq = new UDPMessage(UDPMessage.MessageType.REQUEST, UUID.randomUUID().toString(),
                "purchaseShare", 0, clientEndpoint, "Buy 5 shares of NYKM100925");
        fe.sendClientRequestToSequencer(clientReq);
    }
}
