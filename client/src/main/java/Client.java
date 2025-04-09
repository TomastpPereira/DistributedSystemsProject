import market.CentralRepository;
import market.Market;
import utility.BufferedLog;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import java.net.URL;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Client {

    private static BufferedLog log;
    private static String userID;

    private static Market connectToServer(String userID) {
        String marketPrefix = userID.substring(0, 3);
        try {
            URL wsdlURL = new URL("http://localhost:" + getPortForMarket(marketPrefix) + "/market?wsdl");
            QName qname = new QName("http://market/", "MarketImplService");
            Service service = Service.create(wsdlURL, qname);
            return service.getPort(Market.class);
        } catch (Exception e) {
            System.out.println("Failed to connect to " + marketPrefix + '\n' + e.getMessage());
            return null;
        }
    }

    private static int getPortForMarket(String marketPrefix) {
        return switch (marketPrefix) {
            case "NYK" -> 1098;
            case "LON" -> 1099;
            case "TOK" -> 1097;
            default -> throw new IllegalArgumentException("Invalid market prefix.");
        };
    }

    private static Market connectToMarketForShare(String shareID) {
        try {
            URL wsdlURL = new URL("http://localhost:1096/centralrepository?wsdl");
            QName qname = new QName("http://market/", "CentralRepositoryImplService");
            Service service = Service.create(wsdlURL, qname);
            CentralRepository repository = service.getPort(CentralRepository.class);

            String marketPrefix = repository.getMarketForShare(shareID);
            if (marketPrefix.equals("NONE")) {
                System.out.println("No market found for Share ID: " + shareID);
                return null;
            }

            return connectToServer(marketPrefix);
        } catch (Exception e) {
            System.out.println("Error finding market for share." + e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Shutdown] Cleaning up resources...");
            if (log != null) {
                log.shutdown();
            }
            scanner.close();
            System.out.println("[Shutdown] Cleanup complete.");
        }));

        System.out.println("Welcome to the Distributed Market System");
        System.out.print("Enter your User ID (e.g., ADMNYK1000 or BUYLON1000): ");
        userID = scanner.nextLine().trim();

        String marketPrefix = userID.substring(0, 3);
        if (!(marketPrefix.equals("NYK") || marketPrefix.equals("LON") || marketPrefix.equals("TOK"))) {
            System.out.println("Invalid market prefix.");
            return;
        }

        String fileName = userID.startsWith("ADM") ? "AdminClient" : "BuyerClient";
        log = new BufferedLog("logs/", fileName, userID);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.logEntry("Client", "Shutdown", BufferedLog.RequestResponseStatus.SUCCESS, "N/A", "Client exiting.");
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

        Market stub = connectToServer(userID);
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
                    stub = connectToServer(userID);
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
                    stub = connectToServer(userID);
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
                    stub = connectToServer(userID);
                    System.out.print("Enter Share Type: ");
                    String shareType = scanner.nextLine();

                    String availability = stub.listShareAvailability(shareType);
                    log.logEntry("Client", "listShareAvailability", BufferedLog.RequestResponseStatus.SUCCESS, availability, "Listing shares");
                    System.out.println(availability);
                }
                case 4, 5, 7 -> runSharedTransaction(scanner, choice);
                case 6 -> {
                    stub = connectToServer(userID);
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
                    Market stub = connectToServer(userID);
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
        System.out.print("Enter Share ID: ");
        String shareID = scanner.nextLine();

        Market marketStub = connectToMarketForShare(shareID);
        if (marketStub == null) {
            System.out.println("Failed. No market found.");
            return;
        }

        try {
            switch (operation) {
                case 1 -> {
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
                    System.out.print("Enter Quantity to Sell: ");
                    int qty = scanner.nextInt();
                    scanner.nextLine();
                    String res = marketStub.sellShare(userID, shareID, qty);
                    log.logEntry("Client", "sellShare", BufferedLog.RequestResponseStatus.SUCCESS, res, "Sell share");
                    System.out.println(res);
                }
                case 4, 7 -> {
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
            e.printStackTrace();
        }
    }
}
