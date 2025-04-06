import market.CentralRepository;
import market.Market;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import java.time.LocalDateTime;
import java.util.Scanner;

public class AdminClient {

    private static String logFile;

    public static void log(String message){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true))){
            writer.write(message);
            writer.newLine();
        }
        catch (IOException e){
            System.err.println("Error writing to log file" + logFile);
        }
    }

    private static Market connectToServer(String userID) {
        String marketPrefix = userID.substring(0, 3); // Extract first 3 characters

        try {
            URL wsdlURL = new URL("http://localhost:" + getPortForMarket(marketPrefix) + "/market?wsdl");
            QName qname = new QName("http://market/", "MarketImplService");
            Service service = Service.create(wsdlURL, qname);
            return service.getPort(Market.class);
        } catch (Exception e) {
            System.out.println("Failed to connect to " + marketPrefix);
            e.printStackTrace();
            return null;
        }
    }

    private static int getPortForMarket(String marketPrefix) {
        switch (marketPrefix) {
            case "NYK": return 1098;
            case "LON": return 1099;
            case "TOK": return 1097;
            default: throw new IllegalArgumentException("Invalid market prefix.");
        }
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
            System.out.println("Error finding market for share.");
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        System.out.println("Welcome to the Market Management System. Your are an Admin");

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter Admin ID: ");
        String adminID = scanner.nextLine();

        logFile = "logs/AdminClient_" + adminID + "_log.txt";

        Market stub = connectToServer(adminID);
        if (stub == null) {
            System.out.println("Exiting due to invalid ID.");
            return;
        }

        try{
            programloop:
            while (true){
                System.out.println("\nChoose an Operation to Perform");
                System.out.println("1: Add a Share");
                System.out.println("2: Remove a Share");
                System.out.println("3: List Share Availability");
                System.out.println("4: Purchase a Share");
                System.out.println("5: Sell a Share");
                System.out.println("6: Get Owned Shares");
                System.out.println("7: Swap Shares");
                System.out.println("8: Exit");

                int choice = scanner.nextInt();
                scanner.nextLine();

                switch(choice){
                    case 1: {
                        stub = connectToServer(adminID);
                        System.out.println("Enter Share ID:");
                        String shareID = scanner.nextLine();
                        System.out.println("Enter Share Type (Equity/Bonus/Dividend)");
                        String shareType = scanner.nextLine();
                        System.out.println("Enter Capacity to Add");
                        int capacity = scanner.nextInt();
                        String response = stub.addShare(shareID, shareType, capacity);

                        log(response);
                        System.out.println(response);
                        break;
                    }

                    case 2: {
                        stub = connectToServer(adminID);
                        System.out.print("Enter Share ID: ");
                        String shareID = scanner.nextLine();
                        System.out.print("Enter Share Type (Equity/Bonus/Dividend): ");
                        String shareType = scanner.nextLine();
                        String response = stub.removeShare(shareID, shareType);

                        log(response);
                        System.out.println(response);
                        break;
                    }

                    case 3: {
                        stub = connectToServer(adminID);
                        System.out.print("Enter Share Type (Equity/Bonus/Dividend): ");
                        String shareType = scanner.nextLine();
                        String availability = stub.listShareAvailability(shareType);
                        System.out.println(availability);

                        log(LocalDateTime.now() + "- Success listShareAvailability - shareType "+ shareType);
                        break;
                    }

                    case 4: {
                        System.out.println("Enter Share ID:");
                        String shareID = scanner.nextLine();

                        Market marketStub = connectToMarketForShare(shareID);
                        if (marketStub == null){
                            System.out.println("Purchase failed. No market found");
                            break;
                        }

                        System.out.println("Enter Share Type (Equity/Bonus/Dividend)");
                        String shareType = scanner.nextLine();
                        System.out.println("Enter Quantity to Buy");
                        int numShares = scanner.nextInt();
                        scanner.nextLine();
                        System.out.println("Enter Date (DDMMYY)");
                        String datemonthyear = scanner.nextLine();

                        String response = marketStub.purchaseShare(adminID, shareID, shareType, numShares, datemonthyear);

                        log(response);
                        System.out.println(response);
                        break;
                    }

                    case 5: {
                        System.out.println("Enter Share ID:");
                        String shareID = scanner.nextLine();

                        Market marketStub = connectToMarketForShare(shareID);
                        if (marketStub == null){
                            System.out.println("Purchase failed. No market found");
                            break;
                        }

                        System.out.println("Enter Quantity to Sell");
                        int numShares = scanner.nextInt();
                        String response = marketStub.sellShare(adminID, shareID, numShares);

                        log(response);
                        System.out.println(response);
                        break;
                    }

                    case 6: {

                        stub = connectToServer(adminID);
                        // SHOULD NOT OCCUR, BUT THIS COVERS THE WARNING
                        if (stub == null) {
                            System.out.println("Exiting due to invalid ID.");
                            return;
                        }

                        String owned = stub.getShares(adminID);
                        System.out.println(owned);

                        log(LocalDateTime.now() + " - Success getShares - buyerID "+ adminID);
                        break;
                    }

                    case 7:{
                        System.out.println("Enter Share ID to Remove:");
                        String shareID = scanner.nextLine();

                        Market marketStub = connectToMarketForShare(shareID);
                        if (marketStub == null){
                            System.out.println("Purchase failed. No market found");
                            break;
                        }

                        System.out.println("Enter Share Type to Remove (Equity/Bonus/Dividend)");
                        String shareType = scanner.nextLine();

                        System.out.println("Enter Share ID to Gain:");
                        String newShareID = scanner.nextLine();

                        System.out.println("Enter Share Type to Gain (Equity/Bonus/Dividend)");
                        String newShareType = scanner.nextLine();

                        String response = marketStub.swapShares(adminID, shareID, shareType, newShareID, newShareType);

                        log(response);
                        System.out.println(response);
                        break;
                    }

                    case 8: {
                        System.out.println("Exiting Program!");
                        break programloop;
                    }
                }


            }

            scanner.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
