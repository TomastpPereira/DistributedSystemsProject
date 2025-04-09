import market.MarketImpl;

import javax.xml.ws.Endpoint;

public class TokyoServer {

    Endpoint marketEndpoint;
    UDPServer udpEndpoint;

//    public static void main(String[] args) {
//        MarketImpl market = new MarketImpl();
//        market.initialize("TOK", 1097); // Set market details
//        new UDPServer(1097, market).start();
//        Endpoint.publish("http://localhost:1097/market", market);
//        System.out.println("Tokyo Market (TOK) is live on JAX-WS.");
//    }

    public TokyoServer(String ip, int port){
        MarketImpl market = new MarketImpl();
        market.initialize("TOK", port, ip, port-30);
        udpEndpoint = new UDPServer(port, market);
        udpEndpoint.start();
        String ipPublish = "http://" + ip + ":" + port + "/Tokyo";
        marketEndpoint = Endpoint.publish(ipPublish, market);
    }
}
