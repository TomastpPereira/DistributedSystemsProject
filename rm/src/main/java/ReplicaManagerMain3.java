import io.github.cdimascio.dotenv.Dotenv;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;

public class ReplicaManagerMain3 {

    public static void main(String[] args) throws UnknownHostException {
        Dotenv dotenv = Dotenv.configure()
                .directory(Paths.get(System.getProperty("user.dir")).toString())
                .load();

        ReplicaManager RM3 = new ReplicaManager(InetAddress.getByName(dotenv.get("RM_THREE_IP")), Integer.parseInt(dotenv.get("RM_THREE_PORT")),  "RM3");

    }
}
