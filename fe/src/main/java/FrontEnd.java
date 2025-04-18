import io.github.cdimascio.dotenv.Dotenv;
import network.UDPMessage;
import utility.BufferedLog;

import java.io.*;
import java.net.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class FrontEnd {

    private static final Dotenv dotenv = Dotenv.configure()
            .directory(Paths.get(System.getProperty("user.dir")).toString())
            .load();

    private final List<InetSocketAddress> replicaManagerEndpoints;
    private final InetSocketAddress sequencerEndpoint;

    private final DatagramSocket socket;
    private final BlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private static BufferedLog log;
    private static final ExecutorService taskExecutor = Executors.newFixedThreadPool(10);

    private static long ACK_TIMEOUT;
    private static long REPLICA_RESPONSE_TIMEOUT;

    private sealed interface MessageState permits Received, Sending, WaitForAck, WaitToSequence, Sequenced, WaitToDie, Dead {
    }

    private final class Received implements MessageState {
        InetSocketAddress address;

        Received(InetSocketAddress address) {
            this.address = address;
        }
    }

    private final class Sending implements MessageState {
        InetSocketAddress remoteAddress;

        Sending(InetSocketAddress address) {
            this.remoteAddress = address;
        }
    }

    private final class WaitForAck implements MessageState {
        long sendTime;
        InetSocketAddress remoteAddress;

        WaitForAck(InetSocketAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
            this.sendTime = System.currentTimeMillis();
        }
    }

    private final class WaitToSequence implements MessageState {
        InetSocketAddress address;

        WaitToSequence(InetSocketAddress address) {
            this.address = address;
        }
    }

    private final class Sequenced implements MessageState {
        long sequenceNumber;
        InetSocketAddress clientAddress;
        HashMap<Integer, String> results;
        long creationTime;

        Sequenced(long sequenceNumber, InetSocketAddress clientAddress) {
            this.sequenceNumber = sequenceNumber;
            this.clientAddress = clientAddress;
            this.results = new HashMap<>();
            this.creationTime = System.currentTimeMillis();
        }

        public void addResult(int port, String result) {
            results.put(port, result);
        }

        public boolean isFull() {
            return results.size() == 3;
        }
    }

    private final class WaitToDie implements MessageState {
        long sequenceNumber;
        InetSocketAddress clientAddress;
        HashMap<Integer, String> results;
        long creationTime;

        WaitToDie(Sequenced sc) {
            this.sequenceNumber = sc.sequenceNumber;
            this.clientAddress = sc.clientAddress;
            this.results = sc.results;
            this.creationTime = sc.creationTime;
        }

        public void addResult(int port, String result) {
            results.put(port, result);
        }

        public boolean isFull() {
            return results.size() == 3;
        }
    }

    private final class Dead implements MessageState {
    }

    private class Message {
        UDPMessage message;
        MessageState state;

        Message(UDPMessage message, MessageState state) {
            this.message = message;
            this.state = state;
        }

        public void setState(MessageState state) {
            this.state = state;
        }
    }

    private void sendMessage(UDPMessage message, InetAddress remoteAddress, int remotePort) {
        try {
            byte[] buffer = message.serialize();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, remoteAddress, remotePort);
            socket.send(packet);
            System.out.println("[" + System.currentTimeMillis() + "][Send]" + remoteAddress + ":" + remotePort + " " + message.getPayload());
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    public FrontEnd() throws Exception {
        log = new BufferedLog("logs/", "Front-End", "Front-End");

        this.replicaManagerEndpoints = new ArrayList<>();
        replicaManagerEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_ONE_IP")), Integer.parseInt(dotenv.get("RM_ONE_PORT"))));
        replicaManagerEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_TWO_IP")), Integer.parseInt(dotenv.get("RM_TWO_PORT"))));
        replicaManagerEndpoints.add(new InetSocketAddress(InetAddress.getByName(dotenv.get("RM_THREE_IP")), Integer.parseInt(dotenv.get("RM_THREE_PORT"))));
        this.sequencerEndpoint = new InetSocketAddress(InetAddress.getByName(dotenv.get("SEQUENCER_IP")), Integer.parseInt(dotenv.get("SEQUENCER_PORT")));
        ACK_TIMEOUT = Long.parseLong(dotenv.get("ACK_TIMEOUT"));
        REPLICA_RESPONSE_TIMEOUT = Long.parseLong(dotenv.get("REPLICA_RESPONSE_TIMEOUT"));

        this.socket = new DatagramSocket(Integer.parseInt(dotenv.get("FE_PORT")), InetAddress.getByName(dotenv.get("FE_IP")));
    }

    public void start() {
        new Thread(this::receiveLoop).start();
        new Thread(this::processIncomingMessages).start();
    }

    private void receiveLoop() {
        byte[] buffer = new byte[4096];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
                UDPMessage msg = new UDPMessage(packet.getData(), packet.getLength());
                synchronized (messages) {
                    Message receivedMsg = new Message(msg, new Received(new InetSocketAddress(packet.getAddress(), packet.getPort())));
                    messages.add(receivedMsg);
                }
                System.out.println("[" + System.currentTimeMillis() + "][Received]" + packet.getAddress() + ":" + packet.getPort() + " " + msg.getPayload());
            } catch (IOException e) {
                System.out.println("IOException in Front-End receiveLoop " + e.getMessage());
            }
        }
    }

    private void processIncomingMessages() {
        while (true) {
            synchronized (messages) {
                Iterator<Message> it = messages.iterator();
                while (it.hasNext()) {
                    System.out.println("[" + System.currentTimeMillis() + "] [Number of message to process " + messages.size() + "]");
                    Message processingMessage = it.next();
                    if (processingMessage.state instanceof Received r) {
                        taskExecutor.submit(() -> {
                            if (processingMessage.message.getMessageType() == UDPMessage.MessageType.ACK) {

                                String id = processingMessage.message.getMessageId();
                                processingMessage.setState(new Dead());

                                List<Message> receivedMessages = messages.stream()
                                        .filter(m -> m.message.getMessageId().equals(id) && m.state instanceof WaitForAck w)
                                        .toList();

                                for (Message m : receivedMessages) {
                                    m.setState(new Dead());
                                }
                            } else if (processingMessage.message.getMessageType() == UDPMessage.MessageType.RESPONSE) {  // From Sequencer
                                try {
                                    Map.Entry<InetAddress, Integer> firstEntry = processingMessage.message.getEndpoints().entrySet().iterator().next();
                                    InetSocketAddress clientAddress = new InetSocketAddress(firstEntry.getKey(), firstEntry.getValue());
                                    UDPMessage message = new UDPMessage(processingMessage.message);
                                    processingMessage.message.setMessageType(UDPMessage.MessageType.ACK);
                                    Message ackMessage = new Message(message, new Sending(r.address));
                                    messages.add(ackMessage);

                                    processingMessage.setState(new Sequenced(processingMessage.message.getSequenceNumber(), clientAddress));
                                } catch (Exception e) {
                                    System.out.println(e.getMessage());
                                }
                            } else if (processingMessage.message.getMessageType() == UDPMessage.MessageType.RESULT) { // From RM
                                UDPMessage message = new UDPMessage(processingMessage.message);
                                message.setMessageType(UDPMessage.MessageType.ACK);
                                Message ackMessage = new Message(message, new Sending(r.address));
                                messages.add(ackMessage);
                                processingMessage.setState(new WaitToSequence(r.address));
                            } else if (processingMessage.message.getMessageType() == UDPMessage.MessageType.REQUEST) {// From Client
                                try {
                                    Message clientMessage = new Message(new UDPMessage(processingMessage.message), new Sending(sequencerEndpoint));
                                    messages.add(clientMessage);
                                    processingMessage.setState(new Dead());
                                } catch (Exception e) {
                                    System.out.println("Exception in Front-End handleUDPMessage " + e.getMessage());
                                }
                            } else {
                                System.out.println("Received unknown message type: " + processingMessage);
                            }
                        });
                    } else if (processingMessage.state instanceof Sending s) {
                        taskExecutor.submit(() -> {
                            sendMessage(processingMessage.message, s.remoteAddress.getAddress(), s.remoteAddress.getPort());
                            if (processingMessage.message.getMessageType() == UDPMessage.MessageType.ACK || processingMessage.message.getMessageType() == UDPMessage.MessageType.RESPONSE) {
                                processingMessage.setState(new Dead());
                            } else {
                                processingMessage.setState(new WaitForAck(s.remoteAddress));
                            }
                        });
                    } else if (processingMessage.state instanceof WaitForAck w) {
                        taskExecutor.submit(() -> {
                            long elapsed = System.currentTimeMillis() - w.sendTime;
                            if (elapsed >= ACK_TIMEOUT) {
                                processingMessage.setState(new Sending(w.remoteAddress));
                            }
                        });
                    } else if (processingMessage.state instanceof WaitToSequence w) {
                        taskExecutor.submit(() -> {
                            Optional<Message> maybeSequencedMessage = messages.stream()
                                    .filter(m -> m.state instanceof Sequenced s)
                                    .map(m -> Map.entry(m, (Sequenced) m.state))
                                    .filter(entry -> entry.getValue().sequenceNumber == processingMessage.message.getSequenceNumber())
                                    .map(Map.Entry::getKey)
                                    .findFirst();
                            if (maybeSequencedMessage.isPresent()) {
                                String result = (String) processingMessage.message.getPayload();
                                int serverPort = w.address.getPort();
                                Message sequencedMessage = maybeSequencedMessage.get();
                                if (sequencedMessage.state instanceof Sequenced s) {
                                    s.addResult(serverPort, result);
                                }
                                processingMessage.setState(new Dead());
                            } else {
                                Optional<Message> maybeWaitToDieMessage = messages.stream()
                                        .filter(m -> m.state instanceof WaitToDie s)
                                        .map(m -> Map.entry(m, (WaitToDie) m.state))
                                        .filter(entry -> entry.getValue().sequenceNumber == processingMessage.message.getSequenceNumber())
                                        .map(Map.Entry::getKey)
                                        .findFirst();

                                if (maybeWaitToDieMessage.isPresent()) {
                                    String result = (String) processingMessage.message.getPayload();
                                    int serverPort = w.address.getPort();
                                    Message waitToDieMessage = maybeWaitToDieMessage.get();
                                    if (waitToDieMessage.state instanceof WaitToDie d) {
                                        d.addResult(serverPort, result);
                                    }
                                    processingMessage.setState(new Dead());
                                }
                            }
                        });
                    } else if (processingMessage.state instanceof Sequenced s) {
                        taskExecutor.submit(() -> {
                            if (s.results.size() >= 2) {
                                Map<String, List<Integer>> groupedByResponse = s.results.entrySet()
                                        .stream()
                                        .collect(Collectors.groupingBy(
                                                Map.Entry::getValue,
                                                Collectors.mapping(Map.Entry::getKey,
                                                        Collectors.toList())
                                        ));

                                if (groupedByResponse.size() == 1) { // All the rms sent same result
                                    Map.Entry<String, List<Integer>> entry = groupedByResponse.entrySet().iterator().next();
                                    if (entry.getValue().size() == 2) {
                                        String response = entry.getKey();
                                        UDPMessage message = new UDPMessage(UDPMessage.MessageType.RESPONSE,
                                                processingMessage.message.getAction(), 0,
                                                processingMessage.message.getEndpoints(),
                                                processingMessage.message.getSequenceNumber(),
                                                response
                                        );
                                        messages.add(new Message(message, new Sending(s.clientAddress)));
                                        processingMessage.setState(new WaitToDie(s));
                                    } else if (entry.getValue().size() == 3) {
                                        String response = entry.getKey();
                                        UDPMessage message = new UDPMessage(UDPMessage.MessageType.RESPONSE,
                                                processingMessage.message.getAction(), 0,
                                                processingMessage.message.getEndpoints(),
                                                processingMessage.message.getSequenceNumber(),
                                                response
                                        );
                                        messages.add(new Message(message, new Sending(s.clientAddress)));
                                        processingMessage.setState(new Dead());
                                    }
                                } else if (groupedByResponse.size() == 2) { // One rm sent a different result
                                    List<Integer> lowestGroup = groupedByResponse.values().stream()
                                            .min(Comparator.comparingInt(List::size))
                                            .orElse(List.of());

                                    Map.Entry<String, List<Integer>> highestGroup = groupedByResponse.entrySet()
                                            .stream()
                                            .max(Comparator.comparingInt(entry -> entry.getValue().size()))
                                            .orElse(null);

                                    String response = highestGroup.getKey();

                                    UDPMessage message = new UDPMessage(UDPMessage.MessageType.RESPONSE,
                                            processingMessage.message.getAction(), 0,
                                            processingMessage.message.getEndpoints(),
                                            processingMessage.message.getSequenceNumber(),
                                            response
                                    );
                                    messages.add(new Message(message, new Sending(s.clientAddress)));

                                    StringBuilder errorMsg = new StringBuilder();
                                    for (Integer addr : lowestGroup) {
                                        errorMsg.append(addr).append("::");
                                    }
                                    int length = errorMsg.length();
                                    if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                        errorMsg.setLength(length - 2);
                                    }
                                    UDPMessage rmMessage = new UDPMessage(UDPMessage.MessageType.INCORRECT_RESULT_NOTIFICATION, processingMessage.message.getAction(), 0, processingMessage.message.getEndpoints(), processingMessage.message.getSequenceNumber(), errorMsg.toString());
                                    for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                        messages.add(new Message(rmMessage, new Sending(rmEndpoint)));
                                    }
                                    processingMessage.setState(new Dead());
                                } else if (groupedByResponse.size() == 3) { // All the rms sent different results
                                    List<Integer> unresponsivePorts = replicaManagerEndpoints.stream()
                                            .map(InetSocketAddress::getPort)
                                            .filter(port -> !s.results.containsKey(port))
                                            .toList();

                                    StringBuilder errorMsg = new StringBuilder();
                                    for (Integer addr : unresponsivePorts) {
                                        errorMsg.append(addr).append("::");
                                    }
                                    int length = errorMsg.length();
                                    if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                        errorMsg.setLength(length - 2);
                                    }
                                    UDPMessage message = new UDPMessage(UDPMessage.MessageType.INCORRECT_RESULT_NOTIFICATION, processingMessage.message.getAction(), 0, processingMessage.message.getEndpoints(), processingMessage.message.getSequenceNumber(), errorMsg.toString());
                                    for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                        messages.add(new Message(message, new Sending(rmEndpoint)));
                                    }
                                    processingMessage.setState(new Dead());
                                }

                            } else if ((System.currentTimeMillis() - s.creationTime) >= REPLICA_RESPONSE_TIMEOUT) {
                                List<Integer> unresponsivePorts = replicaManagerEndpoints.stream()
                                        .map(InetSocketAddress::getPort)
                                        .filter(port -> !s.results.containsKey(port))
                                        .toList();

                                StringBuilder errorMsg = new StringBuilder();
                                for (Integer addr : unresponsivePorts) {
                                    errorMsg.append(addr).append("::");
                                }
                                int length = errorMsg.length();
                                if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                    errorMsg.setLength(length - 2);
                                }
                                UDPMessage message = new UDPMessage(UDPMessage.MessageType.CRASH_NOTIFICATION, processingMessage.message.getAction(), 0, processingMessage.message.getEndpoints(), processingMessage.message.getSequenceNumber(), errorMsg.toString());
                                for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                    messages.add(new Message(message, new Sending(rmEndpoint)));
                                }
                                processingMessage.setState(new Dead());

                                // TODO : Send Crash Report to Client
                            }
                        });
                    } else if (processingMessage.state instanceof WaitToDie w) {
                        taskExecutor.submit(() -> {
                            if (w.isFull()) {
                                Map<String, List<Integer>> groupedByResponse = w.results.entrySet()
                                        .stream()
                                        .collect(Collectors.groupingBy(
                                                Map.Entry::getValue,
                                                Collectors.mapping(Map.Entry::getKey,
                                                        Collectors.toList())
                                        ));
                                if (groupedByResponse.size() == 1) {
                                    processingMessage.setState(new Dead());
                                } else {
                                    List<Integer> lowestGroup = groupedByResponse.values().stream()
                                            .min(Comparator.comparingInt(List::size))
                                            .orElse(List.of());

                                    StringBuilder errorMsg = new StringBuilder();
                                    for (Integer addr : lowestGroup) {
                                        errorMsg.append(addr).append("::");
                                    }
                                    int length = errorMsg.length();
                                    if (length >= 2 && errorMsg.substring(length - 2).equals("::")) {
                                        errorMsg.setLength(length - 2);
                                    }
                                    UDPMessage rmMessage = new UDPMessage(UDPMessage.MessageType.INCORRECT_RESULT_NOTIFICATION, processingMessage.message.getAction(), 0, processingMessage.message.getEndpoints(), processingMessage.message.getSequenceNumber(), errorMsg.toString());
                                    for (InetSocketAddress rmEndpoint : replicaManagerEndpoints) {
                                        messages.add(new Message(rmMessage, new Sending(rmEndpoint)));
                                    }
                                    processingMessage.setState(new Dead());
                                }
                            }
                        });
                    } else if (processingMessage.state instanceof Dead d) {
                        it.remove();
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

            }
        }
    }

    public static void main(String[] args) throws Exception {
        new FrontEnd().start();
        System.out.println("[Front-End] Front-End started.");
    }
}
