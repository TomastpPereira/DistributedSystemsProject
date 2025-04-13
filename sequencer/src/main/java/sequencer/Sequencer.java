package sequencer;
import network.UDPMessage;
import network.UDPMessage.MessageType;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Sequencer {

    //CONFIGURATIONS
    private static final int SEQUENCER_PORT = 5000;          // port this sequencer listens on
    private static final List<InetSocketAddress> REPLICAS = Arrays.asList(
            new InetSocketAddress("192.168.1.101", 6001),
            new InetSocketAddress("192.168.1.102", 6002),
            new InetSocketAddress("192.168.1.103", 6003)
    );
    //FRONTEND_ACK_TIMEOUT_MS
    private static final int REPLICA_ACK_WAIT_MS = 3000;     // how long to wait for replica ACKs
    private static final int MAX_RETRIES = 3;                // multicast retry attempts if 3 ack's are not received
    private static final long ID_EXPIRY_MS = 60_000;         // drop saved IDs from front end for duplicate detection after 1 minute
    private static final long CLEANUP_INTERVAL_MS = 30_000;  // how often to remove old IDs of front end messages from record

    // ---- STATE ----
    private final DatagramSocket socket;
    private final AtomicLong seqGenerator = new AtomicLong(0);

    // track front‐end messageIds + receiveTimestamp for duplicate detection
    private final ConcurrentMap<String, Long> receivedIds = new ConcurrentHashMap<>();

    // queue of new, unique requests from front end - order is preserved
    private final BlockingQueue<UDPMessage> requestQueue = new LinkedBlockingQueue<>();

    // pending messages awaiting replica ACKs
    private final ConcurrentMap<Long, UDPMessage> pendingACKs = new ConcurrentHashMap<>();
    // For each in‑flight sequence number, hold a CountDownLatch initialized to replicaCount-3
    private final ConcurrentMap<Long, CountDownLatch> ackLatches = new ConcurrentHashMap<>();
    // Prevent double‑counting the same replica’s ACK by storing replica addresses that have already ACKed.
    private final ConcurrentMap<Long, Set<InetSocketAddress>> ackTrackers = new ConcurrentHashMap<>();
    //Run periodic tasks without blocking your main loops - used for scanning receivedIds and removing old IDs
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // For all udp incoming and outgoing communication
    public Sequencer() throws SocketException {
        this.socket = new DatagramSocket(SEQUENCER_PORT);
    }
    // Starts sequencer’s background work.
    public void start() {
        // 1) schedule periodic cleanup of old message IDs
        scheduler.scheduleAtFixedRate(this::cleanupOldIds,
                CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // 2) start receiver thread - Listens to both front end and replicas
        new Thread(this::receiveLoop, "Sequencer-Receiver").start();

        // 3) start main sequencing loop - Takes new requests in FIFO order, assigns sequence numbers, multicasts, and waits for replica ACKs.
        new Thread(this::processLoop, "Sequencer-Processor").start();

        System.out.println("Sequencer up on port " + SEQUENCER_PORT);
    }

    private void receiveLoop() {
        byte[] buffer = new byte[64_000];
        while (true) {
            try {
                DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
                socket.receive(pkt);

                UDPMessage msg = new UDPMessage(pkt.getData(), pkt.getLength());
                InetSocketAddress sender = new InetSocketAddress(pkt.getAddress(), pkt.getPort());

                switch (msg.getMessageType()) {
                    case REQUEST:
                        handleFrontEndRequest(msg, sender);
                        break;
                    case ACK:
                        handleReplicaAck(msg, sender);
                        break;
                    default:
                        // ignore other message types
                }
            } catch (IOException e) {
                System.err.println("Receive error: " + e.getMessage());
            }
        }
    }

    private void handleFrontEndRequest(UDPMessage msg, InetSocketAddress frontEndAddr) {
        String id = msg.getMessageId();
        boolean isDuplicate = receivedIds.putIfAbsent(id, System.currentTimeMillis()) != null;

        // always ACK back to front end
        MessageType origType = msg.getMessageType();
        msg.setMessageType(MessageType.ACK);
        sendMessage(msg, frontEndAddr);
        // ADDED NEW - to send "RESPONSE" as message type
        msg.setMessageType(MessageType.RESPONSE);
        sendMessage(msg, frontEndAddr);
        msg.setMessageType(origType);

        if (!isDuplicate) {
            // first time: enqueue for sequencing
            requestQueue.offer(msg);
        }
        // else: duplicate which we already ACKed, so drop
    }

    private void handleReplicaAck(UDPMessage msg, InetSocketAddress replicaAddr) {
        long seq = msg.getSequenceNumber();
        CountDownLatch latch = ackLatches.get(seq);
        Set<InetSocketAddress> seen = ackTrackers.get(seq);

        if (latch != null && seen != null) {
            // only count down once per replica
            if (seen.add(replicaAddr)) {
                latch.countDown();
                System.out.printf("Received ACK from %s for seq=%d (%d/%d)%n",
                        replicaAddr, seq,
                        REPLICAS.size() - (int)latch.getCount(),
                        REPLICAS.size());
            }
        }
    }

    private void processLoop() {
        while (true) {
            try {
                // take next request
                UDPMessage msg_request = requestQueue.take();
                long seq_number = seqGenerator.incrementAndGet();

                // stamp and store
                msg_request.setSequenceNumber(seq_number);
                // REDUNDANT CODE - JUST TO MAKE SURE IT IS ALWAYS 'REQUEST'
                msg_request.setMessageType(MessageType.REQUEST);
                pendingACKs.put(seq_number, msg_request);

                // prepare ACK tracking
                CountDownLatch latch = new CountDownLatch(REPLICAS.size());
                ackLatches.put(seq_number, latch);
                ackTrackers.put(seq_number,
                        Collections.newSetFromMap(new ConcurrentHashMap<>()));

                // multicast + retry
                boolean success = false;
                for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                    System.out.printf("Multicasting seq=%d attempt %d%n", seq_number, attempt);
                    multicastToReplicas(msg_request);
                    success = latch.await(REPLICA_ACK_WAIT_MS, TimeUnit.MILLISECONDS);
                    if (success) break;
                    System.out.printf("Timeout waiting for ACKs for seq=%d, retrying...%n", seq_number);
                }

                if (!success) {
                    System.err.printf("Failed to get all ACKs for seq=%d after %d attempts%n",
                            seq_number, MAX_RETRIES);
                } else {
                    System.out.printf("All replicas ACKed seq=%d%n", seq_number);
                }

                // cleanup
                pendingACKs.remove(seq_number);
                ackLatches.remove(seq_number);
                ackTrackers.remove(seq_number);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void multicastToReplicas(UDPMessage msg) {
        for (InetSocketAddress replica : REPLICAS) {
            sendMessage(msg, replica);
        }
    }

    private void sendMessage(UDPMessage msg, InetSocketAddress dest) {
        try {
            byte[] data = msg.serialize();
            DatagramPacket pkt = new DatagramPacket(
                    data, data.length, dest.getAddress(), dest.getPort());
            socket.send(pkt);
        } catch (IOException e) {
            System.err.printf("Failed to send to %s: %s%n", dest, e.getMessage());
        }
    }

    private void cleanupOldIds() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> e : receivedIds.entrySet()) {
            if (now - e.getValue() > ID_EXPIRY_MS) {
                receivedIds.remove(e.getKey());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Sequencer sequencer = new Sequencer();
        sequencer.start();
        System.out.println("Sequencer started successfully!");

    }
}
