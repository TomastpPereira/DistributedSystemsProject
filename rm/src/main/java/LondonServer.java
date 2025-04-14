import market.MarketImpl;

import javax.xml.ws.Endpoint;

public class LondonServer {

    Endpoint marketEndpoint;
    UDPServer udpEndpoint;

//    public static void main(String[] args) {
//        MarketImpl market = new MarketImpl();
//        market.initialize("LON", 1099); // Set market details
//        new UDPServer(1099, market).start();
//        Endpoint.publish("http://localhost:1099/market", market);
//        System.out.println("London Market (LON) is live on JAX-WS.");
//    }

    public LondonServer(String ip, int port){
        MarketImpl market = new MarketImpl();
        market.initialize("LON", port, ip, port-20);
        udpEndpoint = new UDPServer(port, market);
        udpEndpoint.start();
        String ipPublish = "http://" + ip + ":" + port + "/LON";
        marketEndpoint = Endpoint.publish(ipPublish, market);
    }

}
