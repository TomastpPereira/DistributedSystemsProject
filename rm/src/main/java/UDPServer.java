import io.github.cdimascio.dotenv.Dotenv;
import market.MarketImpl;
import market.MarketStateSnapshot;
import network.UDPMessage;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Paths;

/**
 * UDP Server which allows for the messages to be passed between markets.
 */
public class UDPServer extends Thread{

    private final int port;
    private final MarketImpl market;

    private static final Dotenv dotenv = Dotenv.configure()
            .directory(Paths.get(System.getProperty("user.dir")).toString()) //.getParent()
            .load();

    public UDPServer(int port, MarketImpl market){
        this.port = port;
        this.market = market;
    }

    public void run(){
        try (DatagramSocket socket = new DatagramSocket(port)){
            System.out.println("UDP Server connected to port " + port);

            while (true){
                byte[] buffer = new byte[4096];
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                ByteArrayInputStream bais = new ByteArrayInputStream(request.getData(), 0, request.getLength());
                ObjectInputStream ois = new ObjectInputStream(bais);
                UDPMessage udpMessage = (UDPMessage) ois.readObject();

                UDPMessage response = null;

                switch(udpMessage.getAction()){
                    case "SHARES":
                        String buyerID = (String) udpMessage.getPayload();
                        String ownedShares = market.getLocalOwnedShares(buyerID);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "SHARES", 0, null, ownedShares);
                        break;
                    case "CROSS":
                        Object[] crossData = (Object[]) udpMessage.getPayload();
                        market.updateCrossMarket((String) crossData[0], (int) crossData[1], (int) crossData[2]);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "CROSS", 0, null, "Success");
                        break;
                    case "BUY_CHECK":
                        Object[] buyCheckData = (Object[]) udpMessage.getPayload();
                        String result = market.localValidatePurchase(
                                (String) buyCheckData[0],
                                (String) buyCheckData[1],
                                (int) buyCheckData[2]);
                        response = new UDPMessage(UDPMessage.MessageType.RESPONSE, "BUY_CHECK", 0, null, result);
                        break;
                    case "PURCHASE":
                        Object[] purchaseData = (Object[]) udpMessage.getPayload();
                        String purchaseResult = market.purchaseShare(
                                (String) purchaseData[0],
                                (String) purchaseData[1],
                                (String) purchaseData[2],
                                (int) purchaseData[3],
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
                        String params = (String) udpMessage.getPayload();
                        String[] paramsA = params.split(":");
                        String shareID = paramsA[0];
                        shareType = paramsA[1];
                        int cap = Integer.parseInt(paramsA[2]);
                        result = market.addShare(shareID, shareType, cap);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "addShare", 0, null, result);
                        break;
                    case "removeShare":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        shareID = paramsA[0];
                        shareType = paramsA[1];
                        result = market.removeShare(shareID, shareType);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "removeShare", 0, null, result);
                        break;
                    case "listShareAvailability":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        shareType = paramsA[1];
                        market.listShareAvailability(shareType);
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
                        break;
                    case "getShares":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        buyerID = paramsA[0];
                        result = market.getShares(buyerID);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "getShares", 0, null, result);
                        break;
                    case "sellShare":
                        params = (String) udpMessage.getPayload();
                        paramsA = params.split(":");
                        buyerID = paramsA[0];
                        shareID = paramsA[1];
                        cap = Integer.parseInt(paramsA[2]);
                        result = market.sellShare(buyerID, shareID, cap);
                        response = new UDPMessage(UDPMessage.MessageType.RESULT, "sellShare", 0, null, result);
                        break;
                    default:
                        break;
                }

                // Null sequence number
                if (udpMessage.getSequenceNumber() != 0){
                    assert response != null;
                    response.setSequenceNumber(udpMessage.getSequenceNumber());
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(response);
                oos.flush();

                byte[] responseBytes = baos.toByteArray();

                DatagramPacket responseData;
                // Retrieving the info of the FE to send there and not back to the RM
                if (udpMessage.getMessageType() == UDPMessage.MessageType.REQUEST) {
                    InetAddress address;
                    address = InetAddress.getByName(dotenv.get("FE_IP"));
                    int port = Integer.parseInt(dotenv.get("FE_PORT"));
                    responseData = new DatagramPacket(responseBytes, responseBytes.length, address, port);
                    System.out.println("Market sending Message to" + port);
                }
                // If there were no endpoints, send the result back to the sender. Used for the internal UDP messages.
                else {
                    responseData = new DatagramPacket(responseBytes, responseBytes.length, socket.getInetAddress(), socket.getPort());
                    System.out.println("Market sending Message to" + socket.getPort());
                }



                socket.send(responseData);
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

}
