import io.github.cdimascio.dotenv.Dotenv;
import market.Market;
import market.MarketImpl;
import market.MarketStateSnapshot;
import network.UDPMessage;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * UDP Server which allows for the messages to be passed between markets.
 */
public class UDPServer extends Thread{

    private final int port;
    private final MarketImpl market;
    private final DatagramSocket socket;

    private static final Dotenv dotenv = Dotenv.configure()
            .directory(Paths.get(System.getProperty("user.dir")).toString()) //.getParent()
            .load();

    public UDPServer(int port, MarketImpl market){
        this.port = port;
        this.market = market;
        try {
            this.socket = new DatagramSocket(port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void run(){
        try {
            System.out.println("UDP Server connected to port " + port);

            while (true){
                byte[] buffer = new byte[4096];
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                ByteArrayInputStream bais = new ByteArrayInputStream(request.getData(), 0, request.getLength());
                ObjectInputStream ois = new ObjectInputStream(bais);
                UDPMessage udpMessage = (UDPMessage) ois.readObject();

                UDPMessage response = null;

                if (udpMessage.getMessageType() == UDPMessage.MessageType.ACK) {
                    System.out.println("Market Received ACK from FE");

                    // Send an ACK for an ACK?
//                    response = new UDPMessage(udpMessage);
//
//                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                    ObjectOutputStream oos = new ObjectOutputStream(baos);
//                    oos.writeObject(response);
//                    oos.flush();
//
//                    byte[] responseBytes = baos.toByteArray();
//
//                    DatagramPacket responseData = new DatagramPacket(responseBytes, responseBytes.length, request.getAddress(), request.getPort());
//                    socket.send(responseData);

                    continue;
                }

                switch(udpMessage.getAction()){
                    case "SHARES":
                        String buyerID = (String) udpMessage.getPayload();
                        String ownedShares = market.getLocalOwnedShares(buyerID);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "SHARES", 0, null, ownedShares);
                        break;
                    case "CROSS":
                        String crossString = (String) udpMessage.getPayload();
                        String[] crossData = crossString.split(":");
                        market.updateCrossMarket((String) crossData[0], Integer.parseInt(crossData[1]), Integer.parseInt(crossData[2]));
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "CROSS", 0, null, "Success");
                        break;
                    case "BUY_CHECK":
                        String buyCheckString = (String) udpMessage.getPayload();
                        String[] buyCheckData = buyCheckString.split(":");
                        String result = market.localValidatePurchase(
                                (String) buyCheckData[0],
                                (String) buyCheckData[1],
                                 Integer.parseInt(buyCheckData[2]));
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "BUY_CHECK", 0, null, result);
                        break;
                    case "PURCHASE":
                        String purchaseString = (String) udpMessage.getPayload();
                        String[] purchaseData = purchaseString.split(":");
                        String purchaseResult = market.purchaseShare(
                                (String) purchaseData[0],
                                (String) purchaseData[1],
                                (String) purchaseData[2],
                                Integer.parseInt(purchaseData[3]),
                                (String) purchaseData[4]);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "PURCHASE", 0, null, purchaseResult);
                        break;
                    case "AVAILABILITY":
                        String shareType = (String) udpMessage.getPayload();
                        String availability = market.getLocalShareAvailability(shareType);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "AVAILABILITY", 0, null, availability);
                        break;
                    case "DATA-SEND":
                        MarketStateSnapshot snapshotSend = market.getMarketState();
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "DATA", 0, null, snapshotSend);
                        break;
                    case "DATA-RECEIVE":
                        MarketStateSnapshot snapshotReceive = (MarketStateSnapshot) udpMessage.getPayload();
                        market.updateMarketState(snapshotReceive);
                        response = new UDPMessage(UDPMessage.MessageType.ACK, "OK", 0, null, null);
                        break;
                    case "addShare":
                        System.out.println("Market " + port + " is Processing Seq#" + udpMessage.getSequenceNumber());
                        String params = (String) udpMessage.getPayload();
                        String[] paramsA = params.split(":");
                        String shareID = paramsA[0];
                        shareType = paramsA[1];
                        int cap = Integer.parseInt(paramsA[2]);
                        result = market.addShare(shareID, shareType, cap);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "addShare", 0, null, result);
                        response.setSequenceNumber(udpMessage.getSequenceNumber());
                        break;
                    case "removeShare":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        shareID = paramsA[0];
                        shareType = paramsA[1];
                        result = market.removeShare(shareID, shareType);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "removeShare", 0, null, result);
                        response.setSequenceNumber(udpMessage.getSequenceNumber());
                        break;
                    case "listShareAvailability":
                        //TODO: FIX CASE
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        shareType = paramsA[0];
                        result = market.listShareAvailability(shareType);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "listShareAvailability", 0, null, result);
                        response.setSequenceNumber(udpMessage.getSequenceNumber());
                        break;
                    case "purchaseShare":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        buyerID = paramsA[0];
                        shareID = paramsA[1];
                        shareType = paramsA[2];
                        cap = Integer.parseInt(paramsA[3]);
                        String datemonthyear = paramsA[4];
                        result = market.purchaseShare(buyerID, shareID, shareType, cap, datemonthyear);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "listShareAvailability", 0, null, result);
                        response.setSequenceNumber(udpMessage.getSequenceNumber());
                        break;
                    case "swapShares":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        buyerID = paramsA[0];
                        String oldShareID = paramsA[1];
                        String oldShareType = paramsA[2];
                        String newShareID = paramsA[3];
                        String newShareType = paramsA[4];
                        result = market.swapShares(buyerID, oldShareID, oldShareType, newShareID, newShareType);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "swapShares", 0, null, result);
                        response.setSequenceNumber(udpMessage.getSequenceNumber());
                        break;
                    case "getShares":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        buyerID = paramsA[0];
                        result = market.getShares(buyerID);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "getShares", 0, null, result);
                        response.setSequenceNumber(udpMessage.getSequenceNumber());
                        break;
                    case "sellShare":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        buyerID = paramsA[0];
                        shareID = paramsA[1];
                        cap = Integer.parseInt(paramsA[2]);
                        result = market.sellShare(buyerID, shareID, cap);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "sellShare", 0, null, result);
                        response.setSequenceNumber(udpMessage.getSequenceNumber());
                        break;
                    default:
                        break;
                }

                InetAddress addressEnd = InetAddress.getByName(dotenv.get("RM_ONE_IP"));
                Map<InetAddress, Integer> endpoint = new HashMap<>();
                endpoint.put(addressEnd, port);
                response.setEndpoints(endpoint);

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(response);
                oos.flush();

                byte[] responseBytes = baos.toByteArray();

                DatagramPacket responseData;
                // Send to FE if this is a result
                assert response != null;
                if (response.getMessageType() == UDPMessage.MessageType.RESULT) {
                    InetAddress address;
                    address = InetAddress.getByName(dotenv.get("FE_IP"));
                    int port = Integer.parseInt(dotenv.get("FE_PORT"));
                    responseData = new DatagramPacket(responseBytes, responseBytes.length, address, port);
                    System.out.println("Market sending Message to " + port);
                }
                // Else, send the result back to the sender. Used for the internal UDP messages.
                else {
                    responseData = new DatagramPacket(responseBytes, responseBytes.length, request.getAddress(), request.getPort());
                }



                socket.send(responseData);
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

}
