package market;

import io.github.cdimascio.dotenv.Dotenv;
import network.UDPMessage;

import javax.jws.WebService;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.*;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@WebService(endpointInterface = "market.Market")
public class MarketImpl implements Market {

    private static final Dotenv dotenv = Dotenv.configure()
            .directory(Paths.get(System.getProperty("user.dir")).toString()) //.getParent()
            .load();

    private HashMap<String, HashMap<String, Share>> shares; // shareType -> (shareID -> info)
    private HashMap<String, HashMap<String, Integer>> buyerRecords; // buyerID -> (shareID -> # shares)
    private HashMap<String, HashMap<Integer, Integer>> weeklyCrossMarketPurchases; // buyerID -> (week -> count)
    private HashMap<String, HashMap<String, Set<String>>> dailyPurchases; // buyerID -> (date -> shareTypes purchased)
    private final String logPath = "logs/";

    private String market;
    private int port;
    private int RMPort;


    public MarketImpl(){

    }

    public MarketStateSnapshot getMarketState(){
        MarketStateSnapshot thisState = new MarketStateSnapshot();
        thisState.setMarket(market);
        thisState.setShares(shares);
        thisState.setBuyerRecords(buyerRecords);
        thisState.setWeeklyCrossMarketPurchases(weeklyCrossMarketPurchases);
        thisState.setDailyPurchases(dailyPurchases);

        return thisState;
    }

    public void updateMarketState(MarketStateSnapshot snapshot){
        this.shares = snapshot.getShares();
        this.buyerRecords = snapshot.getBuyerRecords();
        this.weeklyCrossMarketPurchases = snapshot.getWeeklyCrossMarketPurchases();
        this.dailyPurchases = snapshot.getDailyPurchases();
    }


    /**
     * New Method to initialize the server object and its parameters now that the constructor must be no-argument
     * @param market    The market associated with this object (NYK, LON or TOK)
     * @param port      The port for this market server.
     */
    public void initialize(String market, int port, String ip, int RMPort){
        this.market = market;
        this.port = port;
        this.RMPort = RMPort;

        //registerWithCentralRepository(ip, centralPort);
        shares = new HashMap<>();
        shares.put("Equity", new HashMap<>());
        shares.put("Bonus", new HashMap<>());
        shares.put("Dividend", new HashMap<>());

        buyerRecords = new HashMap<>();
        weeklyCrossMarketPurchases = new HashMap<>();
        dailyPurchases = new HashMap<>();

        File logDir = new File(logPath);
        if (!logDir.exists()) {
            logDir.mkdir();
        }
    }

    /**
     * Forms a connection between the market and the central repository and registers its port information.
     * Not utilized in the distributed system.
     */
//    private void registerWithCentralRepository(String ip, int port) {
//        try {
//            URL wsdlURL = new URL("http://" + ip + ":" + port + "/centralrepository?wsdl");
//            QName qname = new QName("http://DSMS/", "CentralRepositoryImplService");
//            Service service = Service.create(wsdlURL, qname);
//            CentralRepository repository = service.getPort(CentralRepository.class);
//
//            repository.registerMarketServer(market, "localhost", port);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * Logging function for the market operations.
     *
     * @param filename The string path to the logging file
     * @param message   The message to be logged
     */
    private void log(String filename, String message){
        String path = logPath + market + filename;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, true))) {
            writer.write(message);
            writer.newLine();
        }
        catch (IOException e){
            System.err.println("Error writing to log file: " + path);
            e.printStackTrace();
        }
    }

    /**
     * Adds the given share to the market's shares.
     * In doing this, also registers the share into the central repository.
     *
     * @param shareID   The string ID for the share
     * @param shareType The string type for the share (Equity, Bonus, Dividend)
     * @param capacity  The int capacity to add for the given share
     * @return          Returns a string indicating the success or failure of the operation. This is also logged.
     */
    @Override
    public synchronized String addShare(String shareID, String shareType, int capacity){
        if (!shares.containsKey(shareType)) {
            log("_log.txt", LocalDateTime.now() + "- Failed addShare: Invalid Share Type - shareID:" + shareID + ", shareType: "
                    + shareType + ", capacity: " + capacity);

            return "- Failed addShare: Invalid Share Type - shareID:" + shareID + ", shareType: "
                    + shareType + ", capacity: " + capacity;
        }

        String shareMarketPrefix = shareID.substring(0, 3);
        if (!shareMarketPrefix.equals(market)){
            log("_log.txt", LocalDateTime.now() + "- Failed addShare: Incorrect Market - shareID:" + shareID + ", shareType: "
                    + shareType + ", capacity: " + capacity);
            return "- Failed addShare: Incorrect Market - shareID:" + shareID + ", shareType: "
                    + shareType + ", capacity: " + capacity;
        }

        Map<String, Share> shareMap = shares.get(shareType);
        if (shareMap.containsKey(shareID)) {
            log("_log.txt", LocalDateTime.now() + "- Failed addShare: Share already Exists - shareID:" + shareID + ", shareType: "
                    + shareType + ", capacity: " + capacity);
            return "- Failed addShare: Share already Exists - shareID:" + shareID + ", shareType: "
                    + shareType + ", capacity: " + capacity;
        }

        shareMap.put(shareID, new Share(shareID, shareType, capacity));


        log("_log.txt", LocalDateTime.now() + "- Success addShare - Added share " + shareID + " (" + shareType + ") with capacity " + capacity);
        return "- Success addShare - Added share " + shareID + " (" + shareType + ") with capacity " + capacity;
    }

    /**
     * Removes a share from the market's shares
     *
     * @param shareID   The string ID for the share
     * @param shareType The string type for the share (Equity, Bonus, Dividend)
     * @return          Returns a string indicating the success or failure of the operation. This is also logged.
     */
    @Override
    public synchronized String removeShare(String shareID, String shareType){

        if (!shares.containsKey(shareType)) {
            log("_log.txt", LocalDateTime.now() + "- Failed removeShare: Invalid share type - shareID " + shareID + ", shareType" + shareType);
            return "- Failed removeShare: Invalid share type - shareID " + shareID + ", shareType" + shareType;
        }

        Map<String, Share> shareMap = shares.get(shareType);
        if (!shareMap.containsKey(shareID)) {
            log("_log.txt", LocalDateTime.now() + "- Failed removeShare: Share does not Exist - shareID " + shareID + ", shareType" + shareType);
            return "- Failed removeShare: Share does not Exist - shareID " + shareID + ", shareType" + shareType;
        }

        shareMap.remove(shareID);
        log("_log.txt", LocalDateTime.now() + "- Success removeShare - Removed " + shareID + " (" + shareType + ")");
        return "- Success removeShare - Removed " + shareID + " (" + shareType + ")";

    }

    /**
     * Lists the availability of shares of a given type across all markets.
     * To do so, the getLocalShareAvailability helper method is used, and a UDP connection to the other markets.
     *
     * @param shareType Share type being queried
     * @return          The full list of availability of shares of the given type
     */
    @Override
    public synchronized String listShareAvailability(String shareType) {

        StringBuilder result = new StringBuilder("Market Information: \n");

        if (!shares.containsKey(shareType)) {
            log("_log.txt", LocalDateTime.now() + " - Failed listShareAvailability: Invalid Share Type - shareType: " + shareType);
            return "Invalid Share Type.";
        }


        String address = dotenv.get("RM_ONE_IP");
        // Query other servers
        if (!market.equals("NYK")) {
            result.append("NYK: ").append(requestShareAvailability(shareType, address,RMPort+30)).append("\n");
        }
        if (!market.equals("LON")) {
            result.append("LON: ").append(requestShareAvailability(shareType, address,RMPort+20)).append("\n");
        }
        if (!market.equals("TOK")) {
            result.append("TOK: ").append(requestShareAvailability(shareType, address,RMPort+40)).append("\n");
        }

        result.append(market).append(": ").append(getLocalShareAvailability(shareType)).append("\n");

        log("_log.txt", LocalDateTime.now() + " - Success listShareAvailability - shareType: " + shareType);
        return result.toString();
    }

    /**
     * Helper method which returns the share availability for the particular market object.
     *
     * @param shareType String type of the shares being queried
     * @return  String containing the header for this market alongside the shares available.
     */
    public synchronized String getLocalShareAvailability(String shareType) {

        Map<String, Share> shareMap = shares.get(shareType);
        StringBuilder localAvailability = new StringBuilder();

        for (Share share : shareMap.values()) {
            localAvailability.append(share.shareID).append("(").append(share.availableCount).append("), ");
        }

        log("_log.txt", LocalDateTime.now() + " - Sent LocalShareAvailability - shareType:" + shareType);
        return localAvailability.toString();
    }

    /**
     * Second helper function for share availability which sends a UDP request to the given server to return its share availability.
     *
     * @param shareType     String type of the shares being queried.
     * @param hostReceiver  Host of the server. In this case, always localhost.
     * @param portReceiver  Port of the server.
     * @return              The response data returned from the request.
     */
    private synchronized String requestShareAvailability(String shareType, String hostReceiver, int portReceiver){
        try(DatagramSocket socket = new DatagramSocket()){

            UDPMessage message = new UDPMessage(UDPMessage.MessageType.INNER_REQUEST, "AVAILABILITY", 0, null, shareType);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(message);
            oos.flush();

            InetAddress address = InetAddress.getByName(hostReceiver);

            log("_log.txt", LocalDateTime.now() + " - Sent Request for ShareAvailability - shareType:" + shareType + " port: " + portReceiver);
            DatagramPacket request = new DatagramPacket(baos.toByteArray(), baos.size(), address, portReceiver);
            socket.send(request);

            byte[] buffer = new byte[4096];
            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            socket.receive(response);

            ByteArrayInputStream bais = new ByteArrayInputStream(response.getData(), 0, response.getLength());
            ObjectInputStream ois = new ObjectInputStream(bais);
            UDPMessage udpResponse = (UDPMessage) ois.readObject();

            log("_log.txt", LocalDateTime.now() + " - Received Response for ShareAvailability - shareType:" + shareType + " port: " + portReceiver);

            return (String) udpResponse.getPayload();
        }
        catch (Exception e){
            return "Error contacting " + hostReceiver + ": " + e.getMessage();
        }
    }

    /**
     * Operation allowing a buyer to purchase a given share.
     * Using the entered date, limitations on purchases per day or week are upheld.
     * If a buyer does not belong to this market, the cross-market purchase log is updated and shared to the other servers.
     *
     * @param buyerID   String ID of the buyer
     * @param shareID   String ID of the share
     * @param shareType String type of the share (Equity, Bonus, Dividend)
     * @param shareCount    Int amount of shares to be purchases
     * @param datemonthyear A string in the form DDMMYY on which the share is being purchased.
     * @return          Returns a string indicating the success or failure of the operation. This is also logged.
     */
    @Override
    public synchronized String purchaseShare(String buyerID, String shareID, String shareType, int shareCount, String datemonthyear) {

        String buyerMarket = buyerID.substring(0,3);

        int day = Integer.parseInt(datemonthyear.substring(0,2));
        int month = Integer.parseInt(datemonthyear.substring(2,4));
        int year = Integer.parseInt(datemonthyear.substring(4,6));
        int week = ((month - 1) * 4) + ((day - 1) / 7) + 1;

        // This happens when the purchase is a swap
        if (datemonthyear.equals("000000")){
            week = 9999;
        }

        // Validation
        if (!shares.containsKey(shareType) || !shares.get(shareType).containsKey(shareID)) {
            log("_log.txt", LocalDateTime.now() + " - Failed purchaseShare: Invalid Share - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear);
            return " - Failed purchaseShare: Invalid Share - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear;
        }

        // Check purchase 1 of each type per day
        dailyPurchases.putIfAbsent(buyerID, new HashMap<>());
        dailyPurchases.get(buyerID).putIfAbsent(datemonthyear, new HashSet<>());
        if (dailyPurchases.get(buyerID).get(datemonthyear).contains(shareType)){
            log("_log.txt", LocalDateTime.now() + " - Failed purchaseShare: Buyer Has Already Purchased This Type Today - buyerID "
                    + buyerID + ", shareID " + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear);
            return " - Failed purchaseShare: Buyer Has Already Purchased This Type Today - buyerID "
                    + buyerID + ", shareID " + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear;
        }


        // Check for purchase limit in other markets
        weeklyCrossMarketPurchases.putIfAbsent(buyerID, new HashMap<>());
        weeklyCrossMarketPurchases.get(buyerID).putIfAbsent(week, 0);

        if(!buyerMarket.equals(market)){
            int crossMarketPurchases = weeklyCrossMarketPurchases.get(buyerID).get(week);
            if (crossMarketPurchases >= 3){
                log("_log.txt", LocalDateTime.now() + " - Failed purchaseShare: Buyer Exceeded Weekly Cross Market Purchases - buyerID "
                        + buyerID + ", shareID " + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear);
                return " - Failed purchaseShare: Buyer Exceeded Weekly Cross Market Purchases - buyerID "
                        + buyerID + ", shareID " + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear;
            }
            weeklyCrossMarketPurchases.get(buyerID).put(week, crossMarketPurchases + 1);

            String address = dotenv.get("RM_ONE_IP");
            // Share the cross market purchase to others
            if (!market.equals("NYK")) {
                shareCrossMarket(buyerID, week, crossMarketPurchases + 1, address, RMPort+30);
            }
            if (!market.equals("LON")) {
                shareCrossMarket(buyerID, week, crossMarketPurchases + 1, address, RMPort+20);
            }
            if (!market.equals("TOK")) {
                shareCrossMarket(buyerID, week, crossMarketPurchases + 1, address, RMPort+40);
            }

        }

        Share share = shares.get(shareType).get(shareID);
        int purchasable = Math.min(share.availableCount, shareCount);

        if (purchasable == 0) {
            log("_log.txt", LocalDateTime.now() + " - Failed purchaseShare: No Available Shares - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear);
            return " - Failed purchaseShare: No Available Shares - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear;
        }

        share.availableCount -= purchasable;
        dailyPurchases.get(buyerID).get(datemonthyear).add(shareType);

        // Update buyer's record
        if (buyerRecords.get(buyerID) == null) {
            buyerRecords.put(buyerID, new HashMap<>());
            buyerRecords.get(buyerID).put(shareID, purchasable);
        }
        else {
            buyerRecords.get(buyerID).putIfAbsent(shareID, 0);
            int currentOwned = buyerRecords.get(buyerID).get(shareID);
            buyerRecords.get(buyerID).put(shareID, currentOwned + purchasable);
        }

        log("_log.txt", LocalDateTime.now() + " - Success purchaseShare: Bought " + purchasable + " from Available Shares - buyerID " + buyerID + ", shareID "
                + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear);
        return " - Success purchaseShare: Bought " + purchasable + " from Available Shares - buyerID " + buyerID + ", shareID "
                + shareID + ", shareType" + shareType + ", shareCount" + shareCount + ", datemonthyear " + datemonthyear;
    }

    /**
     * Helper function which updates the cross-market purchases for a buyer
     *
     * @param buyerID ID of the buyer being updated
     * @param week  int week of the year when this purchase is made
     * @param count The new purchase count to be stored.
     */
    public synchronized void updateCrossMarket(String buyerID, int week, int count){
        log("_log.txt", LocalDateTime.now() + " - Starting UpdateCrossMarket: buyerID " + buyerID + " week " + week + " count" + count);
        weeklyCrossMarketPurchases.putIfAbsent(buyerID, new HashMap<>());
        weeklyCrossMarketPurchases.get(buyerID).putIfAbsent(week, 0);

        weeklyCrossMarketPurchases.get(buyerID).put(week, count);
        log("_log.txt", LocalDateTime.now() + " - Success UpdateCrossMarket: buyerID " + buyerID + " week " + week + " count" + count);
    }

    /**
     * Helper method for purchasing which shares to other markets when a cross-market purchase is made.
     *
     * @param buyerID   String ID of the buyer
     * @param week      Int week when the purchase is made
     * @param count     Int cross-market purchase count to share
     * @param hostReceiver  Host of the receiving server. In this case, always localhost
     * @param portReceiver  Port of the receiving server
     */
    private synchronized void shareCrossMarket(String buyerID, int week, int count, String hostReceiver, int portReceiver){
        new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket()) {

                String payload = buyerID + ":" + week + ":" + count;
                UDPMessage message = new UDPMessage(UDPMessage.MessageType.INNER_REQUEST, "CROSS", 0, null, payload);

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(message);
                oos.flush();

                InetAddress address = InetAddress.getByName(hostReceiver);
                DatagramPacket request = new DatagramPacket(baos.toByteArray(), baos.size(), address, portReceiver);
                socket.send(request);
                log("_log.txt", LocalDateTime.now() + " - Sent Update for Cross Market - buyerID:" + buyerID + " week: " + week + " count:" + count +
                        " port: " + portReceiver);

                byte[] buffer = new byte[4096];
                DatagramPacket response = new DatagramPacket(buffer, buffer.length);
                socket.receive(response);

                ByteArrayInputStream bais = new ByteArrayInputStream(response.getData(), 0, response.getLength());
                ObjectInputStream ois = new ObjectInputStream(bais);
                UDPMessage udpResponse = (UDPMessage) ois.readObject();


                log("_log.txt", LocalDateTime.now() + " - Received Response for Cross Market - buyerID:" + buyerID + " week: " + week + " count:" + count +
                        " port: " + portReceiver + "message: " + udpResponse.getPayload());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * Returns a string containing all the shares owned by a buyer across all markets.
     * To retrieve this information for external markets, a UDP message is sent using the requestOwnedShares helper.
     *
     * @param buyerID   ID of the buyer being queried
     * @return          The ownership log of the buyer
     */
    @Override
    public synchronized String getShares(String buyerID) {

        StringBuilder result = new StringBuilder("Shares owned by " + buyerID + ": \n");

        String address = dotenv.get("RM_ONE_IP");
        // Query other servers
        if (!market.equals("NYK")) {
            result.append("NYK: ").append(requestOwnedShares(buyerID, address, RMPort+30)).append("\n");
        }
        if (!market.equals("LON")) {
            result.append("LON: ").append(requestOwnedShares(buyerID, address, RMPort+20)).append("\n");
        }
        if (!market.equals("TOK")) {
            result.append("TOK: ").append(requestOwnedShares(buyerID, address, RMPort+40)).append("\n");
        }

        result.append(market).append(": ").append(getLocalOwnedShares(buyerID)).append("\n");

        log("_log.txt", LocalDateTime.now() + " - Success getShares - buyerID " + buyerID);
        return result.toString();
    }

    /**
     * Helper for get shares which retrieves the buyer's owned shares in this market.
     *
     * @param buyerID   ID of the buyer being queried
     * @return          String log of the market header and owned shares in this market.
     */
    public synchronized String getLocalOwnedShares(String buyerID) {
        Map<String, Integer> shareMap = buyerRecords.get(buyerID);
        StringBuilder owned = new StringBuilder();

        if (shareMap == null)
            return owned.toString();

        for (String share : shareMap.keySet()) {
            owned.append(share).append(" ").append(shareMap.get(share)).append(", ");
        }

        log("_log.txt", LocalDateTime.now() + " - Sent LocalShareAvailability - buyerID:" + buyerID);
        return owned.toString();
    }

    /**
     * Second helper method for getShares which send a UDP message to the other markets requesting the buyer's owned shares
     *
     * @param buyerID       ID of the buyer
     * @param hostReceiver  Host of the receiving server. In this case, always localhost
     * @param portReceiver  Port of the receiving server
     * @return              String containing the response data from the server
     */
    private synchronized String requestOwnedShares(String buyerID, String hostReceiver, int portReceiver){
        try(DatagramSocket socket = new DatagramSocket()){

            UDPMessage message = new UDPMessage(UDPMessage.MessageType.INNER_REQUEST, "SHARES", 0, null, buyerID);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(message);
            oos.flush();

            InetAddress address = InetAddress.getByName(hostReceiver);
            DatagramPacket request = new DatagramPacket(baos.toByteArray(), baos.size(), address, portReceiver);
            socket.send(request);
            log("_log.txt", LocalDateTime.now() + " - Sent Request for Owned Shares - buyerID:" + buyerID + " port: " + portReceiver);

            byte[] buffer = new byte[4096];
            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            socket.receive(response);

            log("_log.txt", LocalDateTime.now() + " - Received Response for Owned Shares - buyerID:" + buyerID + " port: " + portReceiver);

            ByteArrayInputStream bais = new ByteArrayInputStream(response.getData(), 0, response.getLength());
            ObjectInputStream ois = new ObjectInputStream(bais);
            UDPMessage udpResponse = (UDPMessage) ois.readObject();

            return (String) udpResponse.getPayload();
        }
        catch (Exception e){
            return "Error contacting " + hostReceiver + ": " + e.getMessage();
        }
    }

    /**
     * Operation allowing a buyer to sell their owned shares.
     *
     * @param buyerID   ID of the buyer
     * @param shareID   ID of the share
     * @param shareCount    Amount of shares to be sold
     * @return          String indicating the success or failure of the operation. This is also logged.
     * @throws RemoteException
     */
    @Override
    public synchronized String sellShare(String buyerID, String shareID, int shareCount) {
        if (!buyerRecords.containsKey(buyerID) || !buyerRecords.get(buyerID).containsKey(shareID)) {
            log("_log.txt", LocalDateTime.now() + " - Failed sellShares: Share not Owned - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareCount " + shareCount);
            return " - Failed sellShares: Share not Owned - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareCount " + shareCount;
        }

        int ownedShares = buyerRecords.get(buyerID).get(shareID);
        if (ownedShares == 0){
            log("_log.txt", LocalDateTime.now() + " - Failed sellShares: Share not Owned - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareCount " + shareCount);
            return " - Failed sellShares: Share not Owned - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareCount " + shareCount;
        }

        if (ownedShares >= shareCount) {
            buyerRecords.get(buyerID).put(shareID, ownedShares - shareCount);

            // Add back to market availability
            for (Map<String, Share> shareMap : shares.values()) {
                if (shareMap.containsKey(shareID)) {
                    shareMap.get(shareID).availableCount += shareCount;
                    log("_log.txt", LocalDateTime.now() + " - Success sellShares - buyerID " + buyerID + ", shareID "
                            + shareID + ", shareCount " + shareCount);
                    return " - Success sellShares - buyerID " + buyerID + ", shareID "
                            + shareID + ", shareCount " + shareCount;
                }
            }

        }
        else {
            log("_log.txt", LocalDateTime.now() + " - Failed sellShares: Selling More Than Owned - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareCount " + shareCount);
            return " - Failed sellShares: Selling More Than Owned - buyerID " + buyerID + ", shareID "
                    + shareID + ", shareCount " + shareCount;
        }



        log("_log.txt", "!!!!! Sell failed: Market no longer has share " + shareID);
        return "!!!!! Sell failed: Market no longer has share " + shareID;
    }

    /**
     * Operation allowing a buyer to swap an owned share with another share.
     * Internally, checks that the buyer has purchased the share which they are trading.
     * If successful, sends a request to validate whether the requested share is available.
     * If both operations succeed, the owned share is sold and the new share is purchased.
     *
     * @param oldID String ID of the share being traded
     * @param oldType   String type of the share being traded
     * @param newID String ID of the share being acquired
     * @param newType   String type of the share being acquired
     * @return  Success or Failure Message
     */
    public String swapShares(String buyerID, String oldID, String oldType, String newID, String newType){
        if (!shares.containsKey(oldType) || !shares.containsKey(newType)) {
            return "Error: Invalid share type given.";
        }

        // Process Sell Portion - Buyer is connected to this market so no messaging needed
        if (!buyerRecords.containsKey(buyerID) || !buyerRecords.get(buyerID).containsKey(oldID)) {
            log("_log.txt", LocalDateTime.now() + " - Failed swapShares: Share not Owned - buyerID " + buyerID + ", oldID "
                    + oldID);
            return (" - Failed swapShares: Share not Owned - buyerID " + buyerID + ", oldID "
                    + oldID);
        }

        int ownedShares = buyerRecords.get(buyerID).get(oldID);


        // Process Buy Portion
        String shareMarket = newID.substring(0,3);
        String address = dotenv.get("RM_ONE_IP");
        System.out.println("SWAPSHARE - market is " + shareMarket);
        AtomicReference<String> purchaseResult = new AtomicReference<>("");

        int port = 0;
        switch (shareMarket) {
            case "NYK" ->
                    port = RMPort + 30;
            case "LON" ->
                    port = RMPort + 20;
            case "TOK" ->
                    port = RMPort + 40;
            default -> {
                return("- Failed swapShares: Unknown market " + shareMarket);
            }
        }

        AtomicReference<String> toReturn = new AtomicReference<>("");

        int finalPort = port;
        requestValidatePurchase(newID, newType, ownedShares, address, port, validateCallback -> {
            purchaseResult.set(validateCallback);
            System.out.println("Swapshare purchase result:"+ purchaseResult);

            // IF Cant Purchase
            if (!purchaseResult.get().trim().equals("Success")){
                log("_log.txt", LocalDateTime.now() + " - Failed swapShares: Purchase is Not Possible - buyerID" + buyerID + ", oldID " + oldID + ", oldType "
                        + oldType + ", newID " + newID + ", newType" + newType);
                toReturn.set(" - Failed swapShares: Purchase is Not Possible - buyerID" + buyerID + ", oldID " + oldID + ", oldType "
                        + oldType + ", newID " + newID + ", newType" + newType);
            }

            // Success - Process Both
            sellShare(buyerID, oldID, ownedShares);
            // Send message to other server to buy
            String buyResult = "";
            String addressSend = dotenv.get("RM_ONE_IP");

            sendBuyOrder(buyerID, newID, newType, ownedShares, addressSend, finalPort, buyCallback ->{
                toReturn.set(buyCallback);
                log("_log.txt", LocalDateTime.now() + " - Success swapShares - buyerID" + buyerID + ", oldID " + oldID + ", oldType "
                        + oldType + ", newID " + newID + ", newType" + newType);
                toReturn.set(" - Success swapShares - buyerID" + buyerID + ", oldID " + oldID + ", oldType "
                        + oldType + ", newID " + newID + ", newType" + newType);

                System.out.println("Swap Shares Returns -> " + toReturn.get());
            });

        });

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 30000) {
            if (toReturn.get() != null) {
                return toReturn.get();
            }
            try {
                Thread.sleep(10); // short pause to avoid 100% CPU
            } catch (InterruptedException ignored) {}
        }

        return "- Failed swapShares: Timeout waiting for validation and buy.";
    }

    // Deadlock occurs is not using threads because a CrossMarket Update May be Called

    /**
     * Helper method for Swap shares which sends the buy order to the market which holds the new share.
     * This operation is performed in a multithreaded fashion to avoid deadlock caused by cross-market purchases.
     *
     * @param buyerID   String ID of the buyer
     * @param shareID   String ID of the share
     * @param shareType String type of the share
     * @param shareAmount   Int amount of shares being purchased
     * @param hostReceiver  Host of the market server (in this case always localhost)
     * @param port      Int port of the market server
     */
    private void sendBuyOrder(String buyerID, String shareID, String shareType, int shareAmount, String hostReceiver, int port, Consumer<String> callback) {
        new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket(0)) {

                String payload = shareID + ":" + shareType + ":" + shareAmount + ":" + buyerID;
                UDPMessage message = new UDPMessage(UDPMessage.MessageType.INNER_REQUEST, "PURCHASE", 0, null, payload);

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(message);
                oos.flush();

                InetAddress address = InetAddress.getByName(hostReceiver);
                DatagramPacket request = new DatagramPacket(baos.toByteArray(), baos.size(), address, port);
                socket.send(request);
                log("_log.txt", LocalDateTime.now() + " - Sent Request for Purchase - shareID:" + shareID + " shareType:" + shareType +
                        " shareAmount:" + shareAmount + " port: " + port);

                byte[] buffer = new byte[4096];
                DatagramPacket response = new DatagramPacket(buffer, buffer.length);
                socket.receive(response);

                System.out.println("Marker buy order complete");

                ByteArrayInputStream bais = new ByteArrayInputStream(response.getData(), 0, response.getLength());
                ObjectInputStream ois = new ObjectInputStream(bais);
                UDPMessage udpResponse = (UDPMessage) ois.readObject();
                String stringResponse = (String) udpResponse.getPayload();
                callback.accept(stringResponse);

                log("_log.txt", LocalDateTime.now() + " - Received Response for Purchase - shareID:" + shareID + " shareType:" + shareType +
                        " shareAmount:" + shareAmount + " port: " + port + "message: " + stringResponse);

            } catch (Exception e) {
                callback.accept("Error contacting " + hostReceiver + ": " + e.getMessage());
            }
        }).start();
    }


    /**
     * Helper method for the Swap shares method which sends a request to market server to check whether the given shares are purchasable.
     * The response from the server will be a string message headed by either "Success" or "Failure"
     *
     * @param shareID      String ID of the share
     * @param shareType    String type of the share
     * @param shareAmount  Int amount to purchase
     * @param hostReceiver Host of the server (in this case always localhost)
     * @param port         Port of the server
     * @return String response from the server
     */
    private void requestValidatePurchase(String shareID, String shareType, int shareAmount, String hostReceiver, int port, Consumer<String> callback) {
        new Thread(() ->{
            try {
                DatagramSocket socket = new DatagramSocket();
                String payload = shareID + ":" + shareType + ":" + shareAmount;
                UDPMessage message = new UDPMessage(UDPMessage.MessageType.INNER_REQUEST, "BUY_CHECK", 0, null, payload);

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(message);
                oos.flush();

                InetAddress address = InetAddress.getByName(hostReceiver);
                DatagramPacket request = new DatagramPacket(baos.toByteArray(), baos.size(), address, port);
                socket.send(request);
                log("_log.txt", LocalDateTime.now() + " - Sent Request for Purchase Validation - shareID:" + shareID + " shareType:" + shareType +
                        " shareAmount:" + shareAmount + " port: " + port);

                byte[] buffer = new byte[4096];
                DatagramPacket response = new DatagramPacket(buffer, buffer.length);
                socket.receive(response);

                ByteArrayInputStream bais = new ByteArrayInputStream(response.getData(), 0, response.getLength());
                ObjectInputStream ois = new ObjectInputStream(bais);
                UDPMessage udpResponse = (UDPMessage) ois.readObject();
                String stringResponse = (String) udpResponse.getPayload();
                System.out.println("Success in deserializing - Result: "+ stringResponse);

                log("_log.txt", LocalDateTime.now() + " - Received Response for Purchase Validation - shareID:" + shareID + " shareType:" + shareType +
                        " shareAmount:" + shareAmount + " port: " + port + "message: " + stringResponse);

                callback.accept(stringResponse);
            } catch (Exception e) {
                callback.accept("Error contacting " + hostReceiver + ": " + e.getMessage());
            }
        }).start();
    }


    /**
     * Helper for the Swap shares method which checks if the server has the request share and amount available.
     *
     * @param shareID   String ID of the share
     * @param shareType String type of the share
     * @param shareAmount   Int amount to purchase
     * @return  Success, if the share and amount are available. Failure, otherwise.
     */
    public synchronized String localValidatePurchase(String shareID, String shareType, int shareAmount){

        if (!shares.containsKey(shareType) || !shares.get(shareType).containsKey(shareID)) {
            return "Failure - Not Available";
        }

        int available = shares.get(shareType).get(shareID).availableCount;
        if (available< shareAmount)
            return "Failure - Not Enough";

        return "Success";
    }
}
