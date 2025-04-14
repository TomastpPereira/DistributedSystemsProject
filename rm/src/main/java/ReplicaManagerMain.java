import io.github.cdimascio.dotenv.Dotenv;

import java.net.InetAddress;
import java.nio.file.Paths;

public class ReplicaManagerMain {

    public static void main(String[] args) throws Exception {

        Dotenv dotenv = Dotenv.configure()
                .directory(Paths.get(System.getProperty("user.dir")).getParent().toString())
                .load();



        ReplicaManager RM1 = new ReplicaManager(InetAddress.getByName(dotenv.get("RM_ONE_IP")), Integer.parseInt(dotenv.get("RM_ONE_PORT")), "RM1");
        ReplicaManager RM2 = new ReplicaManager(InetAddress.getByName(dotenv.get("RM_TWO_IP")), Integer.parseInt(dotenv.get("RM_TWO_PORT")), "RM2");
        ReplicaManager RM3 = new ReplicaManager(InetAddress.getByName(dotenv.get("RM_THREE_IP")), Integer.parseInt(dotenv.get("RM_THREE_PORT")),  "RM3");

//      InetAddress ip = InetAddress.getByName("localhost");

//      ReplicaManager RM1 = new ReplicaManager(ip, 7001, "RM1");
//      ReplicaManager RM2 = new ReplicaManager(ip, 7002, "RM2");
//      ReplicaManager RM3 = new ReplicaManager(ip, 7003, "RM3");

    }
}
