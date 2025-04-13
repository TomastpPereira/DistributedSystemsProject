import io.github.cdimascio.dotenv.Dotenv;
import market.Market;
import utility.BufferedLog;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Scanner;

public class Client {

    private static BufferedLog log;
    private static String userID;

    private static Market feStub = null;

    private static Market connectToFeService() {
        try {
            URL wsdlURL = new URL("http://localhost:" + Dotenv.configure()
                    .directory(Paths.get(System.getProperty("user.dir")).getParent().toString()).load().get("FE_SERVICE_PORT") + "/feservice?wsdl");
            QName qname = new QName("http://market/", "FeServiceService");
            Service service = Service.create(wsdlURL, qname);
            return service.getPort(Market.class);
        } catch (Exception e) {
            System.out.println("Failed to connect to the FE service: " + e.getMessage());
            return null;
        }
    }

    private static Market getFeStub() {
        if (feStub == null) {
            feStub = connectToFeService();
        }
        return feStub;
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Welcome to the Distributed Market System");
        System.out.print("Enter your User ID (e.g., ADMNYK1000 or BUYLON1000): ");
        userID = scanner.nextLine().trim();

        String marketPrefix = userID.substring(3, 6);
        if (!(marketPrefix.equals("NYK") || marketPrefix.equals("LON") || marketPrefix.equals("TOK"))) {
            System.out.println("Invalid market prefix.");
            return;
        }

        String fileName = userID.startsWith("ADM") ? "AdminClient" : "BuyerClient";
        log = new BufferedLog("logs/", fileName, userID);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.logEntry("Client", "Shutdown", BufferedLog.RequestResponseStatus.SUCCESS, "N/A", "Client exiting.");
            scanner.close();
            log.shutdown();
        }));

        if (userID.startsWith("ADM")) {
            runAdminClient(scanner);
        } else if (userID.startsWith("BUY")) {
            runBuyerClient(scanner);
        } else {
            System.out.println("Invalid user type (must start with ADM or BUY)");
        }
    }

    private static void runAdminClient(Scanner scanner) {
        System.out.println("Admin Access Granted");

        Market stub = getFeStub();
        if (stub == null) {
            System.out.println("Exiting due to connection failure.");
            return;
        }

        while (true) {
            System.out.println("\nChoose an Operation to Perform");
            System.out.println("1: Add Share");
            System.out.println("2: Remove Share");
            System.out.println("3: List Share Availability");
            System.out.println("4: Purchase Share");
            System.out.println("5: Sell Share");
            System.out.println("6: Get Owned Shares");
            System.out.println("7: Swap Shares");
            System.out.println("8: Exit");

            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1 -> {
                    stub = getFeStub();
                    System.out.print("Enter Share ID: ");
                    String shareID = scanner.nextLine();
                    System.out.print("Enter Share Type: ");
                    String shareType = scanner.nextLine();
                    System.out.print("Enter Capacity: ");
                    int capacity = scanner.nextInt();
                    scanner.nextLine();

                    assert stub != null;
                    String response = stub.addShare(shareID, shareType, capacity);
                    log.logEntry("Client", "addShare", BufferedLog.RequestResponseStatus.SUCCESS, response, "Add share requested");
                    System.out.println(response);
                }
                case 2 -> {
                    stub = getFeStub();
                    System.out.print("Enter Share ID: ");
                    String shareID = scanner.nextLine();
                    System.out.print("Enter Share Type: ");
                    String shareType = scanner.nextLine();

                    assert stub != null;
                    String response = stub.removeShare(shareID, shareType);
                    log.logEntry("Client", "removeShare", BufferedLog.RequestResponseStatus.SUCCESS, response, "Remove share requested");
                    System.out.println(response);
                }
                case 3 -> {
                    stub = getFeStub();
                    System.out.print("Enter Share Type: ");
                    String shareType = scanner.nextLine();

                    String availability = stub.listShareAvailability(shareType);
                    log.logEntry("Client", "listShareAvailability", BufferedLog.RequestResponseStatus.SUCCESS, availability, "Listing shares");
                    System.out.println(availability);
                }
                case 4, 5, 7 -> runSharedTransaction(scanner, choice);
                case 6 -> {
                    stub = getFeStub();
                    assert stub != null;
                    String owned = stub.getShares(userID);
                    log.logEntry("Client", "getShares", BufferedLog.RequestResponseStatus.SUCCESS, owned, "Get owned shares");
                    System.out.println(owned);
                }
                case 8 -> {
                    System.out.println("Exiting Admin Mode.");
                    return;
                }
                default -> System.out.println("Invalid choice.");
            }
        }
    }

    private static void runBuyerClient(Scanner scanner) {
        System.out.println("Buyer Access Granted");

        while (true) {
            System.out.println("\nChoose an Operation to Perform");
            System.out.println("1: Purchase Share");
            System.out.println("2: Sell Share");
            System.out.println("3: Get Owned Shares");
            System.out.println("4: Swap Shares");
            System.out.println("5: Exit");

            int choice = scanner.nextInt();
            scanner.nextLine();

            switch (choice) {
                case 1, 2, 4 -> runSharedTransaction(scanner, choice);
                case 3 -> {
                    Market stub = getFeStub();
                    assert stub != null;
                    String owned = stub.getShares(userID);
                    log.logEntry("Client", "getShares", BufferedLog.RequestResponseStatus.SUCCESS, owned, "Get owned shares");
                    System.out.println(owned);
                }
                case 5 -> {
                    System.out.println("Exiting Buyer Mode.");
                    return;
                }
                default -> System.out.println("Invalid choice.");
            }
        }
    }

    private static void runSharedTransaction(Scanner scanner, int operation) {
        String shareID;

        Market marketStub = getFeStub();
        if (marketStub == null) {
            System.out.println("Failed. No market found.");
            return;
        }

        try {
            switch (operation) {
                case 1 -> {
                    System.out.print("Enter Share ID: ");
                    shareID = scanner.nextLine();
                    System.out.print("Enter Share Type: ");
                    String shareType = scanner.nextLine();
                    System.out.print("Enter Quantity to Buy: ");
                    int qty = scanner.nextInt();
                    scanner.nextLine();
                    System.out.print("Enter Date (DDMMYY): ");
                    String date = scanner.nextLine();
                    String res = marketStub.purchaseShare(userID, shareID, shareType, qty, date);
                    log.logEntry("Client", "purchaseShare", BufferedLog.RequestResponseStatus.SUCCESS, res, "Purchase share");
                    System.out.println(res);
                }
                case 2 -> {
                    System.out.print("Enter Share ID: ");
                    shareID = scanner.nextLine();
                    System.out.print("Enter Quantity to Sell: ");
                    int qty = scanner.nextInt();
                    scanner.nextLine();
                    String res = marketStub.sellShare(userID, shareID, qty);
                    log.logEntry("Client", "sellShare", BufferedLog.RequestResponseStatus.SUCCESS, res, "Sell share");
                    System.out.println(res);
                }
                case 4, 7 -> {
                    System.out.print("Enter Share ID to Remove: ");
                    shareID = scanner.nextLine();
                    System.out.print("Enter Share Type to Remove: ");
                    String shareType = scanner.nextLine();
                    System.out.print("Enter New Share ID: ");
                    String newShareID = scanner.nextLine();
                    System.out.print("Enter New Share Type: ");
                    String newShareType = scanner.nextLine();
                    String res = marketStub.swapShares(userID, shareID, shareType, newShareID, newShareType);
                    log.logEntry("Client", "swapShares", BufferedLog.RequestResponseStatus.SUCCESS, res, "Swap shares");
                    System.out.println(res);
                }
            }
        } catch (Exception e) {
            log.logEntry("Client", "Transaction", BufferedLog.RequestResponseStatus.FAILURE, "N/A", "Transaction failed: " + e.getMessage());
            System.out.println(e.getMessage());
        }
    }
}
