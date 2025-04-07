
import market.CentralRepositoryImpl;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;
import java.net.InetSocketAddress;


public class CentralRepositoryServer {

    Endpoint centralEndpoint;

    public static void main(String[] args) {
        CentralRepositoryImpl repository = new CentralRepositoryImpl();

        Endpoint.publish("http://localhost:1096/centralrepository", repository);
        System.out.println("Central Repository is live on JAX-WS.");
    }

    public CentralRepositoryServer(String ip, int port){
        CentralRepositoryImpl repository = new CentralRepositoryImpl();
        String ipPublish = "http://" + ip + ":" + Integer.toString(port) + "/centralrepository";
        centralEndpoint = Endpoint.publish(ipPublish, repository);
    }

    //TODO: Modify market registration to the central repository
}
