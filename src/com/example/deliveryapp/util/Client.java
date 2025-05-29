package com.example.deliveryapp.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.stream.Collectors;

public class Client implements Runnable {

    private String host;
    private int port;
    private String role;
    private String action;
    private ObjectOutputStream out;
    private ObjectInputStream in;


    private final Object lock = new Object();

    public Client(String host, int port, String message, String role) {
        this.host = host;
        this.port = port;
        String[] parts = message.split("::", 2);
        this.role = role;
        this.action = parts.length > 1 ? parts[1] : "";
    }


    public void run() {

        while (true) {

            String jobID = UUID.randomUUID().toString();

            if (role.equalsIgnoreCase("manager")) {

                try {

                    Scanner scanner = new Scanner(System.in);

                    while (true) {

                        if (this.action.equalsIgnoreCase("json")) {

                            System.out.println("Enter the name of the JSON file (e.g. store1.json):");
                            String fileName = scanner.nextLine();

                            File file = new File("jsonFiles/" + fileName);

                            if (!file.exists()) {

                                System.err.println("File not found: " + fileName);

                                return;

                            }

                            String jsonContent;
                            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                                jsonContent = reader.lines().collect(Collectors.joining("\n"));
                            } catch (IOException e) {
                                System.err.println("Error reading JSON: " + e.getMessage());
                                return;
                            }

                            Gson gson = new Gson();
                            Store store;
                            try {
                                store = gson.fromJson(jsonContent, Store.class);
                            } catch (JsonSyntaxException e) {
                                System.err.println("Invalid JSON syntax: " + e.getMessage());
                                return;
                            }

                            if (store == null || store.getStoreName() == null || store.getProducts() == null) {

                                System.err.println("[Manager] Invalid JSON: missing required fields.");
                                return;

                            }

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(store, action, jobID));
                                out.flush();

                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("add_available_product")) {

                            Scanner in = new Scanner(System.in);

                            String name, product;
                            int quantity;

                            System.out.println("Please insert the name of the store you want to add the available product to");
                            System.out.print("> ");
                            name = in.nextLine();
                            System.out.println("Please insert the name of the product you want to make available");
                            System.out.print("> ");
                            product = in.nextLine();

                            do {
                                System.out.println("Please insert the available quantity for the product");
                                System.out.print("> ");
                                quantity = in.nextInt();
                                in.nextLine();
                            } while (quantity <= 0);

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(name + "_" + product + "_" + quantity, action,jobID));
                                out.flush();

                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("remove_available_product")) {

                            Scanner in = new Scanner(System.in);

                            String name, product;

                            System.out.println("Please insert the name of the store you want to remove the available product from");
                            System.out.print("> ");
                            name = in.nextLine();
                            System.out.println("Please insert the name of the product you want to make un-available");
                            System.out.print("> ");
                            product = in.nextLine();

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(name + "_" + product, action,jobID));
                                out.flush();

                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("add_new_product")) {

                            Scanner in = new Scanner(System.in);

                            String storeName, productName, productType;
                            double price;
                            int availableAmount;

                            System.out.println("Please insert the name of the store you want to add the new product to");
                            System.out.print("> ");
                            storeName = in.nextLine();
                            System.out.println("Please insert the name of the product you want to add to the store");
                            System.out.print("> ");
                            productName = in.nextLine();
                            System.out.println("Please insert the type of the product");
                            System.out.print("> ");
                            productType = in.nextLine();

                            do {
                                System.out.println("Please insert the available quantity for the product");
                                System.out.print("> ");
                                availableAmount = in.nextInt();
                                in.nextLine();
                            } while (availableAmount <= 0);

                            do {
                                System.out.println("Please insert the price of the product");
                                System.out.print("> ");
                                price = in.nextDouble();
                                in.nextLine();
                            } while (price <= 0);

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(storeName + "_" + productName + "_" + productType + "_" + price + "_" + availableAmount, action,jobID));
                                out.flush();

                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("remove_old_product")) {

                            Scanner in = new Scanner(System.in);

                            String storeName, productName;

                            System.out.println("Please insert the name of the store you want to remove the product from");
                            System.out.print("> ");
                            storeName = in.nextLine();
                            System.out.println("Please insert the name of the product you want to remove from the store");
                            System.out.print("> ");
                            productName = in.nextLine();

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(storeName + "_" + productName, action,jobID));
                                out.flush();

                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("total_sales_store")) {

                            Scanner in = new Scanner(System.in);

                            String category;

                            System.out.println("Please insert the food category you want to search the total sales of");
                            System.out.print("> ");
                            category = in.nextLine();

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(category, action,jobID));
                                out.flush();

                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("total_sales_product")) {

                            Scanner in = new Scanner(System.in);

                            String category;

                            System.out.println("Please insert the product category you want to search the total sales of");
                            System.out.print("> ");
                            category = in.nextLine();

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(category, action,jobID));
                                out.flush();

                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        }

                    }

                } catch (RuntimeException e) {
                    throw new RuntimeException(e);
                }

            } else {

                try {

                    Scanner in = new Scanner(System.in);

                    while (true) {

                        if (this.action.equalsIgnoreCase("showcase_stores")) {

                            String longitude1, latitude1;

                            double longitude;
                            while (true) {
                                System.out.println("Please insert the longitude of your location");
                                System.out.print("> ");
                                try {
                                    longitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            double latitude;
                            while (true) {
                                System.out.println("Please insert the latitude of your location");
                                System.out.print("> ");
                                try {
                                    latitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            longitude1 = String.valueOf(longitude);
                            latitude1 = String.valueOf(latitude);

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(longitude1 + "_" + latitude1, action, jobID));
                                out.flush();
                                System.out.println(clientSocket.getLocalSocketAddress());

                                printSearchResults(clientSocket);

                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }


                        } else if (this.action.equalsIgnoreCase("search_food_preference")) {

                            String longitude1, latitude1, preference;

                            double longitude;
                            while (true) {
                                System.out.println("Please insert the longitude of your location");
                                System.out.print("> ");
                                try {
                                    longitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();

                                }
                            }

                            double latitude;
                            while (true) {
                                System.out.println("Please insert the latitude of your location");
                                System.out.print("> ");
                                try {
                                    latitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            longitude1 = String.valueOf(longitude);
                            latitude1 = String.valueOf(latitude);

                            System.out.println("Please insert the food category of your preference");
                            System.out.print("> ");
                            preference = in.nextLine();

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(longitude1 + "_" + latitude1 + "_" + preference, action, jobID));
                                out.flush();
                                System.out.println(clientSocket.getLocalSocketAddress());

                                printSearchResults(clientSocket);
                                break;


                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("search_ratings")) {

                            String longitude1, latitude1, preference;

                            double longitude;
                            while (true) {
                                System.out.println("Please insert the longitude of your location");
                                System.out.print("> ");
                                try {
                                    longitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();

                                }
                            }

                            double latitude;
                            while (true) {
                                System.out.println("Please insert the latitude of your location");
                                System.out.print("> ");
                                try {
                                    latitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            longitude1 = String.valueOf(longitude);
                            latitude1 = String.valueOf(latitude);

                            int stars;
                            while (true) {
                                System.out.println("Please insert how many stars you want the stores to have");
                                System.out.print("> ");
                                try {
                                    stars = Integer.parseInt(in.next());
                                    in.nextLine();
                                    if (stars > 0 && stars <= 5) {
                                        break;
                                    }
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            preference = String.valueOf(stars);

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(longitude1 + "_" + latitude1 + "_" + preference, action, jobID));
                                out.flush();

                                printSearchResults(clientSocket);
                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("search_price_range")) {

                            String longitude1, latitude1, preference;

                            double longitude;
                            while (true) {
                                System.out.println("Please insert the longitude of your location");
                                System.out.print("> ");
                                try {
                                    longitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();

                                }
                            }

                            double latitude;
                            while (true) {
                                System.out.println("Please insert the latitude of your location");
                                System.out.print("> ");
                                try {
                                    latitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            longitude1 = String.valueOf(longitude);
                            latitude1 = String.valueOf(latitude);

                            do {
                                System.out.println("Please insert the price range of your preference (e.g., $)");
                                System.out.print("> ");
                                preference = in.nextLine();
                            } while (!preference.equals("$") && !preference.equals("$$") && !preference.equals("$$$"));

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(longitude1 + "_" + latitude1 + "_" + preference, action, jobID));
                                out.flush();

                                printSearchResults(clientSocket);
                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("purchase_product")) {

                            String longitude1, latitude1, store, product;

                            double longitude;
                            while (true) {
                                System.out.println("Please insert the longitude of your location");
                                System.out.print("> ");
                                try {
                                    longitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();

                                }
                            }

                            double latitude;
                            while (true) {
                                System.out.println("Please insert the latitude of your location");
                                System.out.print("> ");
                                try {
                                    latitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            longitude1 = String.valueOf(longitude);
                            latitude1 = String.valueOf(latitude);

                            System.out.println("Please insert the name of the store of which you want to make the purchase");
                            System.out.print("> ");
                            store = in.nextLine();

                            System.out.println("Please insert the name of the product you want to purchase");
                            System.out.print("> ");
                            product = in.nextLine();

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(longitude1 + "_" + latitude1 + "_" + store + "_" + product, action, jobID));
                                out.flush();

                                printSearchResults(clientSocket);
                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        } else if (this.action.equalsIgnoreCase("rate_store")) {

                            String longitude1, latitude1, store, preference;

                            double longitude;
                            while (true) {
                                System.out.println("Please insert the longitude of your location");
                                System.out.print("> ");
                                try {
                                    longitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();

                                }
                            }

                            double latitude;
                            while (true) {
                                System.out.println("Please insert the latitude of your location");
                                System.out.print("> ");
                                try {
                                    latitude = Double.parseDouble(in.next());
                                    in.nextLine();
                                    break;
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            longitude1 = String.valueOf(longitude);
                            latitude1 = String.valueOf(latitude);

                            System.out.println("Please insert the name of the store you want to rate");
                            System.out.print("> ");
                            store = in.nextLine();

                            int stars;
                            while (true) {
                                System.out.println("Please insert how many stars you want to rate the store with");
                                System.out.print("> ");
                                try {
                                    stars = Integer.parseInt(in.next());
                                    in.nextLine();
                                    if (stars > 0 && stars <= 5) {
                                        break;
                                    }
                                } catch (NumberFormatException ignore) {
                                    System.out.println("Invalid input");
                                    in.nextLine();
                                }
                            }

                            preference = String.valueOf(stars);

                            try (Socket clientSocket = new Socket(host, port);){

                                out = new ObjectOutputStream(clientSocket.getOutputStream());
                                out.writeObject(new ActionWrapper(longitude1 + "_" + latitude1 + "_" + store + "_" + preference, action, jobID));
                                out.flush();

                                printSearchResults(clientSocket);
                                break;

                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }

                        }

                    }

                } catch (RuntimeException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }


                break;

            }

            break;

        }

    }

    private static void printSearchResults(Socket clientSocket) throws IOException, ClassNotFoundException {

        ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        Object obj = in.readObject();
        ActionWrapper w = (ActionWrapper) obj;
        String resAction = w.getAction();
        Object resObj = w.getObject();

        System.out.println(resObj.toString());

        if (resAction.equalsIgnoreCase("final_results")) {

            List<Store> finalResults = (List<Store>) resObj;

            if (finalResults.isEmpty()) {
                System.out.println("No stores found matching your criteria.");
            } else {

                System.out.println("Found " + finalResults.size() + " stores");

                for (Store store : finalResults) {
                    System.out.println(">> " + store.getStoreName());
                }

            }
        }
    }

}


