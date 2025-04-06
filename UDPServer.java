package DSMS;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

/**
 * UDP Server which allows for the messages to be passed between markets.
 * The UDP server currently serves 3 message types associated with the following tags:
 * SHARES - Returns the owned shares for the requested buyer
 * CROSS - Updates the buyer's cross-market purchase log
 * blank - Share the local share availability of the market
 */
public class UDPServer extends Thread{

    private final int port;
    private final MarketImpl market;

    public UDPServer(int port, MarketImpl market){
        this.port = port;
        this.market = market;
    }

    public void run(){
        try (DatagramSocket socket = new DatagramSocket(port)){
            System.out.println("UDP Server connected to port " + port);


            while (true){
                byte[] buffer = new byte[1000];
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                String requestString = new String(request.getData(), 0, request.getLength());
                System.out.println("UDP request string: " + requestString);
                String response;

                if (requestString.startsWith("SHARES:")){
                    String buyerID = requestString.split(":")[1];
                    response = market.getLocalOwnedShares(buyerID);
                } else if (requestString.startsWith("CROSS:")) {
                    String[] split = requestString.split(":");
                    String buyerID = split[1];
                    int week = Integer.parseInt(split[2]);
                    int count = Integer.parseInt(split[3]);
                    System.out.println("sending");
                    market.updateCrossMarket(buyerID, week, count);
                    System.out.println("sent");
                    response = "Success";
                } else if (requestString.startsWith("BUY_CHECK:")) {
                    String[] split = requestString.split(":");
                    String shareID = split[1];
                    String shareType = split[2];
                    int shareAmount = Integer.parseInt(split[3]);
                    response = market.localValidatePurchase(shareID, shareType, shareAmount);
                } else if (requestString.startsWith("PURCHASE:")) {
                    String[] split = requestString.split(":");
                    String shareID = split[1];
                    String shareType = split[2];
                    int shareAmount = Integer.parseInt(split[3]);
                    String buyerID = split[4];
                    response = market.purchaseShare(buyerID, shareID, shareType, shareAmount, "000000");
                }else {
                    response = market.getLocalShareAvailability(requestString);
                }

                byte[] responseBytes = response.getBytes();
                DatagramPacket responseData = new DatagramPacket(
                        responseBytes, responseBytes.length, request.getAddress(), request.getPort()
                );

                socket.send(responseData);
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
