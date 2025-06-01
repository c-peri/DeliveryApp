package com.example.deliveryapp.worker;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.example.deliveryapp.reducer.ActionsForReducer;
import com.example.deliveryapp.util.*;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;

import static com.example.deliveryapp.util.IPConfig.IP_ADDRESS;

public class ActionsForWorkers implements Runnable {

    private String host;
    private int port;
    private Object received;
    private Map<String, Store> storeMap;
    private int workerId;

    public ActionsForWorkers(String host, int port, Object received, Map<String, Store> storeMap, int workerId) {
        this.host = host;
        this.port = port;
        this.received = received;
        this.storeMap = storeMap;
        this.workerId = workerId;
    }

    ActionWrapper wrapper;
    Object obj;
    String action,opt;
    String jobID;

    public void run() {

        wrapper = (ActionWrapper) received;
        obj = wrapper.getObject();
        action = wrapper.getAction();
        jobID = wrapper.getJobID();
        Object lock = JobCoordinator.getLock(UUID.fromString(jobID));
        List<Store> localStores = new ArrayList<>(storeMap.values());

        String confirmationMsg = "";

        if (action.equalsIgnoreCase("json")) {

            Store store = (Store) obj;
            String storeName = store.getStoreName();

            try {

                if (!storeMap.containsKey(storeName.toLowerCase())) {

                    storeMap.put(storeName.toLowerCase(), store);
                    confirmationMsg = storeName + " has been added";

                    System.out.println("[Worker-" + workerId + "] Added store : '"+storeName+"' to its map successfully");

                } else {

                    confirmationMsg = storeName + " has already been added.";

                    throw new Exception();

                }

            } catch (Exception e) {
                System.err.println("Store already added to the map.");
            }

            new Thread(new ActionsForReducer(IP_ADDRESS, 5000, "manager_confirmation", confirmationMsg, jobID )).start();

        } else if (action.equalsIgnoreCase("add_available_product")){

            opt = (String) obj;
            String[] parts = opt.split("_", 3);
            String storeName = parts[0];
            String product = parts.length > 1 ? parts[1] : "";
            String temp = parts.length > 2 ? parts[2] : "";
            int quantity = Integer.parseInt(temp);

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                boolean found = false;

                synchronized (store) {
                    List<Product> products;
                    products = store.getProducts();
                    for (Product temp1 : products) {
                        if (temp1.getProductName().equalsIgnoreCase(product)) {
                            found = true;
                            temp1.setAvailability(true);
                            temp1.setAvailableAmount(quantity);
                            confirmationMsg = product + " is now available in store " + storeName + " with quantity " + quantity;
                            System.out.println("[Worker-" + workerId + "] Changed available amount to: " + quantity + " successfully");
                            break;
                        }
                    }
                }

                if (!found) {

                    confirmationMsg = product + " does not exist in store: '"+storeName+".";
                    System.err.println("Product does not exist");

                }

            } else {

                confirmationMsg = storeName + "store does not exist.";
                System.err.println("Store does not exist");

            }

            new Thread(new ActionsForReducer(IP_ADDRESS, 5000, "manager_confirmation", confirmationMsg, jobID )).start();

        } else if (action.equalsIgnoreCase("remove_available_product")){

            opt = (String) obj;
            String[] parts = opt.split("_", 2);
            String storeName = parts[0];
            String product = parts.length > 1 ? parts[1] : "";

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                boolean found = false;

                synchronized (store) {

                    List<Product> products;
                    products = store.getProducts();

                    for (Product temp1 : products) {

                        if (temp1.getProductName().equalsIgnoreCase(product)) {

                            found = true;

                            if (temp1.getAvailableAmount() != 0) {

                                temp1.setAvailability(false);
                                temp1.setAvailableAmount(0);
                                confirmationMsg = "Product '" + product + "' in store '" + storeName + "' has been made unavailable to customers.";
                                System.out.println("[Worker-" + workerId + "] Made product: '" + product + "' unavailable to clients successfully");

                            } else {

                                confirmationMsg = "Product '" + product + "' already unavailable to customers.";
                                System.err.println("Product already unavailable to customers.");


                            }

                            break;

                        }

                    }

                }

                if (!found) {

                    confirmationMsg = product + " does not exist in store: '"+storeName+"'.";
                    System.err.println("Product does not exist");

                }

            } else {

                confirmationMsg = storeName + " does not exist.";
                System.err.println("Store does not exist");

            }

            new Thread(new ActionsForReducer(IP_ADDRESS, 5000, "manager_confirmation", confirmationMsg, jobID )).start();

        } else if (action.equalsIgnoreCase("add_new_product")){

            opt = (String) obj;
            String[] parts = opt.split("_", 5);
            String storeName = parts[0];
            String productName = parts.length > 1 ? parts[1] : "";
            String productType = parts.length > 2 ? parts[2] : "";
            String temp2 = parts.length > 3 ? parts[3] : "";
            double price = Double.parseDouble(temp2);
            String temp = parts.length > 4 ? parts[4] : "";
            int quantity = Integer.parseInt(temp);

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                boolean found = false;

                synchronized (store) {
                    List<Product> products;
                    products = store.getProducts();
                    for (Product temp1 : products) {
                        if (temp1.getProductName().equalsIgnoreCase(productName)) {
                            found = true;
                            confirmationMsg = "Product '" + productName + "' already exists in store '" + storeName + "'.";
                            System.err.println("Product: " + productName + " already exists");
                            break;
                        }
                    }
                }

                if (!found) {

                    Product pr = new Product(productName,productType,quantity,price);
                    pr.setClientAvailability(false);
                    List<Product> prs = store.getProducts();
                    prs.add(pr);
                    store.setProducts(prs);
                    confirmationMsg = "Product '" + productName + "' added to store '" + storeName + "' successfully!";
                    System.out.println("[Worker-" + workerId + "] Added product: '"+productName+"' to store: '"+storeName+"' successfully");

                }

            } else {

                confirmationMsg = storeName + " does not exist.";
                System.err.println("Store does not exist");

            }

            new Thread(new ActionsForReducer(IP_ADDRESS, 5000, "manager_confirmation", confirmationMsg, jobID )).start();

        } else if (action.equalsIgnoreCase("remove_old_product")){

            opt = (String) obj;
            String[] parts = opt.split("_", 2);
            String storeName = parts[0];
            String productName = parts.length > 1 ? parts[1] : "";

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                boolean found = false;

                synchronized (store) {
                    List<Product> products;
                    products = store.getProducts();
                    for (Product temp1 : products) {
                        if (temp1.getProductName().equalsIgnoreCase(productName)) {
                            found = true;
                            temp1.setClientAvailability(true);
                            store.setProducts(products);
                            confirmationMsg = "Product '" + productName + "' removed from store '" + storeName + "' successfully!";
                            System.out.println("[Worker-" + workerId + "] Removed product: '" + productName + "' from store: '" + storeName + "' successfully");
                            break;
                        }
                    }
                }

                if (!found) {

                    confirmationMsg = "Product '" + productName + "' does not exist.";
                    System.err.println("Product: "+productName+" does not exist");

                }

            } else {

                confirmationMsg = storeName + " does not exist.";
                System.err.println("Store does not exist");

            }

            new Thread(new ActionsForReducer(IP_ADDRESS, 5000, "manager_confirmation", confirmationMsg, jobID )).start();

        } else if (action.equalsIgnoreCase("showcase_stores")){

            opt = (String) obj;
            String[] parts = opt.split("_", 3);

            String lon = parts[0];
            double longitude = Double.parseDouble(lon);

            String lat = parts[1];
            double latitude =Double.parseDouble(lat);

            StoreMapper mapper = new StoreMapper(latitude, longitude, FilterMode.LOCATION, null);
            List< AbstractMap.SimpleEntry<String, Store> > mapped = mapper.map(localStores);

            try {

                Socket socket = new Socket(host, port);
                ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                ActionWrapper w = new ActionWrapper(mapped, "mapped_store_results", ((ActionWrapper) received).getJobID());
                objOut.writeObject(w);
                objOut.flush();

                synchronized (lock) {
                    while (!JobCoordinator.getStatus(UUID.fromString(jobID)).equals("COMPLETED")) {
                        lock.wait(500);
                    }
                }

            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }

        } else if (action.equalsIgnoreCase("search_food_preference")) {

            opt = (String) obj;
            String[] parts = opt.split("_", 4);

            String lon = parts[0];
            double longitude = Double.parseDouble(lon);

            String lat = parts[1];
            double latitude =Double.parseDouble(lat);

            String food_type = parts[2];

            StoreMapper mapper = new StoreMapper(latitude, longitude, FilterMode.LOCATION_AND_CATEGORY, null);
            mapper.setFoodCategory(food_type);
            List< AbstractMap.SimpleEntry<String, Store> > mapped = mapper.map(localStores);

            try {

                Socket socket = new Socket(host, port);
                ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                ActionWrapper w = new ActionWrapper(mapped, "mapped_store_results", ((ActionWrapper) received).getJobID());
                objOut.writeObject(w);
                objOut.flush();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else if (action.equalsIgnoreCase("search_ratings")) {

            opt = (String) obj;
            String[] parts = opt.split("_", 4);

            String lon = parts[0];
            double longitude = Double.parseDouble(lon);

            String lat = parts[1];
            double latitude =Double.parseDouble(lat);

            String st = parts[2];
            int rating = Integer.parseInt(st);

            StoreMapper mapper = new StoreMapper(latitude, longitude, FilterMode.LOCATION_AND_STARS, null);
            mapper.setMinStars(rating);
            List<AbstractMap.SimpleEntry<String, Store> > mapped = mapper.map(localStores);

            try {

                Socket socket = new Socket(host, port);
                ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                ActionWrapper w = new ActionWrapper(mapped, "mapped_store_results", ((ActionWrapper) received).getJobID());
                objOut.writeObject(w);
                objOut.flush();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else if (action.equalsIgnoreCase("search_price_range")) {

            opt = (String) obj;
            String[] parts = opt.split("_", 4);

            String lon = parts[0];
            double longitude = Double.parseDouble(lon);

            String lat = parts[1];
            double latitude =Double.parseDouble(lat);

            String range = parts[2];

            StoreMapper mapper = new StoreMapper(latitude, longitude, FilterMode.LOCATION_AND_PRICE_RANGE, null);
            mapper.setPriceRange(range);
            List< AbstractMap.SimpleEntry<String, Store> > mapped = mapper.map(localStores);

            try {

                Socket socket = new Socket(host, port);
                ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                ActionWrapper w = new ActionWrapper(mapped, "mapped_store_results", ((ActionWrapper) received).getJobID());
                objOut.writeObject(w);
                objOut.flush();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else if (action.equalsIgnoreCase("purchase_product")) {

            PurchaseDetails purchaseDetails = (PurchaseDetails) obj;

            double longitude = purchaseDetails.getLongitude();
            double latitude = purchaseDetails.getLatitude();
            String storeName = purchaseDetails.getStoreName();
            Map<String, Integer> productsToPurchase = purchaseDetails.getProductsToPurchase();

            StringBuilder confirmationMsgBuilder = new StringBuilder();
            boolean purchaseSuccessfulOverall = true;

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                double distance = GeoUtils.haversine(latitude, longitude, store.getLatitude(), store.getLongitude());

                if (distance <= 5.0) {

                    synchronized (store) {
                        List<Product> storeProducts = store.getProducts();
                        for (Map.Entry<String, Integer> entry : productsToPurchase.entrySet()) {
                            String productName = entry.getKey();
                            int quantityToBuy = entry.getValue();
                            boolean productFound = false;
                            String currentProductConfirmation = "";
                            for (Product productInStore : storeProducts) {
                                if (productInStore.getProductName().equalsIgnoreCase(productName)) {
                                    productFound = true;
                                    int availableAmount = productInStore.getAvailableAmount();
                                    if (availableAmount >= quantityToBuy) {
                                        productInStore.setAvailableAmount(availableAmount - quantityToBuy);
                                        productInStore.setProductSales(productInStore.getProductSales() + quantityToBuy);
                                        currentProductConfirmation = "Purchased " + quantityToBuy + " of '" + productName + "' from '" + storeName + "'.";
                                        System.out.println("[Worker-" + workerId + "] Purchased " + quantityToBuy + " of product: '" + productName + "' from store: '" + storeName + "' successfully");
                                    } else {
                                        currentProductConfirmation = "Failed to purchase " + quantityToBuy + " of '" + productName + "'. Only " + availableAmount + " available.";
                                        System.err.println("[Worker-" + workerId + "] Not enough quantity for product: '" + productName + "'. Available: " + availableAmount + ", Requested: " + quantityToBuy);
                                        purchaseSuccessfulOverall = false;
                                    }
                                    break;
                                }
                            }
                            if (!productFound) {
                                currentProductConfirmation = "Product '" + productName + "' does not exist in store '" + storeName + "'.";
                                System.err.println("[Worker-" + workerId + "] Product: '" + productName + "' not found in store: '" + storeName + "'.");
                                purchaseSuccessfulOverall = false;
                            }
                            confirmationMsgBuilder.append(currentProductConfirmation).append("\n");
                        }
                        store.setProducts(storeProducts);
                    }

                } else {

                    confirmationMsgBuilder.append("Store '").append(storeName).append("' is out of range (distance: ").append(String.format("%.2f", distance)).append("km).");
                    purchaseSuccessfulOverall = false;

                }

            } else {

                confirmationMsgBuilder.append("Store '").append(storeName).append("' does not exist.");
                purchaseSuccessfulOverall = false;

            }

            String finalConfirmationMsg = confirmationMsgBuilder.toString().trim();

            if (finalConfirmationMsg.isEmpty()) {
                finalConfirmationMsg = "No purchase attempts were made.";
            } else if (purchaseSuccessfulOverall) {
                finalConfirmationMsg = "All items processed successfully:\n" + finalConfirmationMsg;
            } else {
                finalConfirmationMsg = "Some issues occurred during purchase:\n" + finalConfirmationMsg;
            }

            try {

                Socket socketToReducer = new Socket(host, port);
                ObjectOutputStream objOutToReducer = new ObjectOutputStream(socketToReducer.getOutputStream());

                ActionWrapper wToReducer = new ActionWrapper(finalConfirmationMsg, "confirmation_from_worker", wrapper.getJobID());

                objOutToReducer.writeObject(wToReducer);
                objOutToReducer.flush();

                synchronized (lock) {
                    while (!JobCoordinator.getStatus(UUID.fromString(jobID)).equals("COMPLETED")) {
                        lock.wait(500);
                    }
                }

            } catch (IOException e) {

                System.err.println("[Worker-" + workerId + "] IOException when sending purchase confirmation: " + e.getMessage());

                throw new RuntimeException(e);

            } catch (InterruptedException e) {

                System.err.println("[Worker-" + workerId + "] InterruptedException during purchase confirmation job coordination: " + e.getMessage());
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);

            }

        } else if (action.equalsIgnoreCase("rate_store")) {

            opt = (String) obj;
            String[] parts = opt.split("_", 5);

            String lon = parts[0];
            double longitude = Double.parseDouble(lon);

            String lat = parts[1];
            double latitude =Double.parseDouble(lat);

            String storeName =  parts[2];

            String starsString = parts[3];
            int stars = Integer.parseInt(starsString);

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                double distance = GeoUtils.haversine(latitude, longitude, store.getLatitude(), store.getLongitude());

                if (distance <= 5.0) {

                    synchronized (store) {
                        store.addStarRating(stars);
                    }

                    if (stars == 1) {
                        confirmationMsg = "You gave " + stars + " star to " + storeName + ".  \uD83D\uDE22";
                    } else if (stars == 2){
                        confirmationMsg = "You gave " + stars + " stars to " + storeName + ".  \uD83D\uDE14";
                    } else if (stars == 3){
                        confirmationMsg = "You gave " + stars + " stars to " + storeName + ".  \uD83E\uDD28";
                    } else if (stars == 4){
                        confirmationMsg = "You gave " + stars + " stars to " + storeName + "!  \uD83D\uDE00";
                    } else {
                        confirmationMsg = "You gave " + stars + " stars to " + storeName + "!  \uD83E\uDD73 \uD83E\uDD29";
                    }

                } else {
                    confirmationMsg = "Store out of range.";
                }

            } else {
                confirmationMsg = "Store does not exist";
            }

            try {

                Socket socketToReducer = new Socket(host, port);
                ObjectOutputStream objOutToReducer = new ObjectOutputStream(socketToReducer.getOutputStream());

                ActionWrapper wToReducer = new ActionWrapper(confirmationMsg, "confirmation_from_worker", wrapper.getJobID());

                objOutToReducer.writeObject(wToReducer);
                objOutToReducer.flush();

                synchronized (lock) {
                    while (!JobCoordinator.getStatus(UUID.fromString(jobID)).equals("COMPLETED")) {
                        lock.wait(500);
                    }
                }

            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }

        } else if (action.equalsIgnoreCase("total_sales_store")) {

            opt = (String) obj;
            String[] parts = opt.split("_", 2);

            StoreMapper mapper = new StoreMapper(0.0, 0.0, parts[0], FilterMode.SALES_STORE);
            List< AbstractMap.SimpleEntry<String, Store> > mapped = mapper.map(localStores);

            System.out.println(mapped);

            try {

                Socket socket = new Socket(host, port);
                ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                ActionWrapper w = new ActionWrapper(mapped, "mapped_store_results1", ((ActionWrapper) received).getJobID());
                objOut.writeObject(w);
                objOut.flush();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else if (action.equalsIgnoreCase("total_sales_product")){

            opt = (String) obj;
            String[] parts = opt.split("_", 2);

            StoreMapper mapper = new StoreMapper(0.0, 0.0, parts[0], FilterMode.SALES_PRODUCT);
            List< AbstractMap.SimpleEntry<String, Store> > mapped = mapper.map(localStores);

            try {

                Socket socket = new Socket(host, port);
                ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                ActionWrapper w = new ActionWrapper(mapped, "mapped_store_results_"+parts[0], ((ActionWrapper) received).getJobID());
                objOut.writeObject(w);
                objOut.flush();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

    }

}

