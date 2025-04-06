
import market.CentralRepositoryImpl;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;


public class CentralRepositoryServer {

    public static void main(String[] args) {
        CentralRepositoryImpl repository = new CentralRepositoryImpl();

        Endpoint.publish("http://localhost:1096/centralrepository", repository);
        System.out.println("Central Repository is live on JAX-WS.");
    }
}
