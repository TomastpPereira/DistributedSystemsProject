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
    private BlockingQueue<Message> recieveMessage = new LinkedBlockingQueue<>();
    private BlockingQueue<SendMessage> sentMessages = new LinkedBlockingQueue<>();
    private BlockingQueue<ClientRequest> sequencerQueue = new LinkedBlockingQueue<>();
    private static ExecutorService taskExecutor = Executors.newFixedThreadPool(10);
    private static BufferedLog log;

    private static int fePort;

    private static long ACK_TIMEOUT;
    private static long REPLICA_RESPONSE_TIMEOUT;

    public FrontEnd(InetSocketAddress sequencerEndpoint, List<InetSocketAddress> replicaManagerEndpoints) throws SocketException {
        this.sequencerEndpoint = sequencerEndpoint;
        this.replicaManagerEndpoints = replicaManagerEndpoints;
        this.socket = new DatagramSocket(fePort);
        fePort = Integer.parseInt(Dotenv.load().get("FE_PORT"));
        ACK_TIMEOUT = Long.parseLong(Dotenv.load().get("ACK_TIMEOUT"));
        REPLICA_RESPONSE_TIMEOUT = Long.parseLong(Dotenv.load().get("REPLICA_RESPONSE_TIMEOUT"));
        log = new BufferedLog("logs/", "Front-End", "Front-End");
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

        public SendMessage(boolean isSend, UDPMessage message, InetSocketAddress endpoint) {
            super(message, endpoint);
            this.isSend = isSend;
        }

        public SendMessage(UDPMessage message, InetSocketAddress endpoint) {
            super(message, endpoint);
            this.isSend = false;
        }

        public void sent() {
            this.isSend = true;
        }

        public boolean isSend() {
            return this.isSend;
        }
    }

    private class ClientRequest {
        final Long sequenceNumber;
        final long timestamp;
        ArrayList<ReplicateResponse> replicaResponses;

        public ClientRequest(Long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            this.timestamp = System.currentTimeMillis();
            this.replicaResponses = new ArrayList<>();
        }

        public void addResponse(ReplicateResponse response) {
            if (!isFull()) {
                replicaResponses.add(response);
            }
        }

        private boolean isFull() {
            return replicaResponses.size() == 3;
        }

        public ReplicateResponseMetric analyzeResponses() {
            Map<String, List<InetSocketAddress>> groupedByResult = new HashMap<>();
            for (ReplicateResponse response : replicaResponses) {
                groupedByResult
                        .computeIfAbsent(response.result, k -> new ArrayList<>())
                        .add(response.replicaManagerEndpoint);
            }

            String majorityResult = null;
            List<InetSocketAddress> matched = new ArrayList<>();

            for (Map.Entry<String, List<InetSocketAddress>> entry : groupedByResult.entrySet()) {
                if (entry.getValue().size() >= 2) {
                    majorityResult = entry.getKey();
                    matched = new ArrayList<>(entry.getValue());
                    break;
                }
            }

            List<InetSocketAddress> mismatched = new ArrayList<>();
            if (majorityResult != null && matched.size() != replicaResponses.size()) {
                for (ReplicateResponse response : replicaResponses) {
                    if (!response.result.equals(majorityResult)) {
                        mismatched.add(response.replicaManagerEndpoint);
                    }
                }
            } else if (majorityResult == null) {
                for (ReplicateResponse response : replicaResponses) {
                    mismatched.add(response.replicaManagerEndpoint);
                }
            }
            return new ReplicateResponseMetric(majorityResult, new ArrayList<>(matched), new ArrayList<>(mismatched));
        }

    }

    private record ReplicateResponseMetric(String majorityResult, ArrayList<InetSocketAddress> matchedInetSocketAddress,
                                           ArrayList<InetSocketAddress> errorInetSocketAddress) {
    }

    private class ReplicateResponse {
        final InetSocketAddress replicaManagerEndpoint;
        final String result;
        final long timestamp;

        public ReplicateResponse(InetSocketAddress replicaManagerEndpoint, String result) {
            this.replicaManagerEndpoint = replicaManagerEndpoint;
            this.result = result;
            this.timestamp = System.currentTimeMillis();
        }
    }

    private Map<InetAddress, Integer> getReplicaManagerEndpointsMap() {
        Map<InetAddress, Integer> map = new HashMap<>();
        for (InetSocketAddress addr : replicaManagerEndpoints) {
            map.put(addr.getAddress(), addr.getPort());
        }
        return map;
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
                recieveMessage.add(new Message(msg, new InetSocketAddress(packet.getAddress(), packet.getPort())));
            } catch (IOException e) {
                System.out.println("IOException in Front-End receiveLoop " + e.getMessage());
            }
        }
    }

    private void sendLoop() {
        while (true) {
            try {
                SendMessage msg = sentMessages.take(); // TODO : Can't use take
                if (!msg.isSend) {
                    taskExecutor.submit(() -> {
                          // TODO : Send Message
//                        Map<InetAddress, Integer> endpoints = new HashMap<>();
//                        endpoints.put(InetAddress.getByName(Dotenv.load().get("FE_SERVICE_IP")), Integer.parseInt(Dotenv.load().get("FE_SERVICE_PORT")));
//                        UDPMessage udpMessage = new UDPMessage(UDPMessage.MessageType.CLIENT_REQUEST, msg.split("::")[0], 0, endpoints, msg);
//                        byte[] buffer = udpMessage.serialize();
//                        InetAddress receiverAddress = InetAddress.getByName(Dotenv.load().get("FE_IP"));
//                        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, receiverAddress, Integer.parseInt(Dotenv.load().get("FE_PORT")));
//                        socket.send(packet);
                    });
                } else if ((System.currentTimeMillis() - msg.timeSent) > Integer.parseInt(Dotenv.load().get("ACK_TIMEOUT"))) {
                    taskExecutor.submit(() -> {
                        // TODO : resend
                    });
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        }
    }

    private void processIncomingMessages() {
        while (true) {
            try {
                Message msg = recieveMessage.take(); // TODO : Can't use take
                taskExecutor.submit(() -> handleUDPMessage(msg));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void checkResultFound() {
        while (true) {
            try {
                ClientRequest cr = sequencerQueue.take(); // TODO : Can't use take
                taskExecutor.submit(() -> {
                    ReplicateResponseMetric metric = cr.analyzeResponses();
                    // TODO : Do the work
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void handleAck(String msgId) {
        sentMessages.removeIf(msgId); // TODO: Fix me
    }

    private void handleUDPMessage(Message msg) {
        switch (msg.message.getMessageType()) {
            case ACK:
                handleAck(msg.message.getMessageId());
                break;
            case RESPONSE:
                sequencerQueue.add(new ClientRequest(msg.message.getSequenceNumber()));
                break;
            case RESULT:
                ClientRequest cr = sequencerQueue.stream().filter(req -> req.sequenceNumber == msg.message.getSequenceNumber()).findFirst().orElse(null);
                if (cr != null) {
                    if (msg.message.getPayload() instanceof String str) {
                        cr.addResponse(new ReplicateResponse(msg.endpoint, str));
                    }
                } else {
                    System.out.println("Sequencer number not found! Issue with sequencer");
                }
                break;
            case CLIENT_REQUEST:
                // TODO : Add to Sent Message
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

        InetSocketAddress sequencerEndpoint = new InetSocketAddress(InetAddress.getByName(Dotenv.load().get("SEQUENCER_PORT")), Integer.parseInt(Dotenv.load().get("SEQUENCER_PORT")));

        List<InetSocketAddress> rmEndpoints = new ArrayList<>();
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(Dotenv.load().get("RM_ONE_IP")), Integer.parseInt(Dotenv.load().get("RM_ONE_PORT"))));
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(Dotenv.load().get("RM_TWO_IP")), Integer.parseInt(Dotenv.load().get("RM_TWO_PORT"))));
        rmEndpoints.add(new InetSocketAddress(InetAddress.getByName(Dotenv.load().get("RM_THREE_PORT")), Integer.parseInt(Dotenv.load().get("RM_THREE_PORT"))));

        FrontEnd fe = new FrontEnd(sequencerEndpoint, rmEndpoints);
        fe.start();
    }
}
