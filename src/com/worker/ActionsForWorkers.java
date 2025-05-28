package com.worker;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.util.*;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ActionsForWorkers implements Runnable {

    private String host;
    private int port;
    private Object received;
    private HashMap<String, Store> storeMap;
    private int workerId;

    public ActionsForWorkers(String host, int port, Object received, HashMap<String, Store> storeMap, int workerId) {
        this.host = host;
        this.port = port;
        this.received = received;
        this.storeMap = storeMap;
        this.workerId = workerId;
    }

    ActionWrapper wrapper;
    Object obj;
    String action,opt;

    public void run() {

        wrapper = (ActionWrapper) received;
        obj = wrapper.getObject();
        action = wrapper.getAction();
        List<Store> localStores = new ArrayList<>(storeMap.values());

        if (action.equalsIgnoreCase("json")) {

            Store store = (Store) obj;
            String storeName = store.getStoreName();

            try {

                if (!storeMap.containsKey(storeName.toLowerCase())) {

                    storeMap.put(storeName.toLowerCase(), store);
                    System.out.println("[Worker-" + workerId + "] Added store : '"+storeName+"' to its map successfully");

                } else {
                    throw new Exception();
                }

            } catch (Exception e) {

                System.err.println("Store already added");

            }

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
                List<Product> products;
                products = store.getProducts();

                for (Product temp1 : products) {

                    if (temp1.getProductName().equalsIgnoreCase(product)) {

                        found = true;

                        temp1.setAvailability(true);
                        temp1.setAvailableAmount(quantity);
                        System.out.println("[Worker-" + workerId + "] Changed available amount to: "+quantity+" successfully");

                        break;

                    }

                }

                if (!found) {

                    System.err.println("Product does not exist");

                }

            } else {

                System.err.println("Store does not exist");

            }

        } else if (action.equalsIgnoreCase("remove_available_product")){

            opt = (String) obj;
            String[] parts = opt.split("_", 2);
            String storeName = parts[0];
            String product = parts.length > 1 ? parts[1] : "";

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                boolean found = false;

                List<Product> products;
                products = store.getProducts();

                for (Product temp1 : products) {

                    if (temp1.getProductName().equals(product)) {

                        found = true;

                        if (temp1.getAvailableAmount() != 0){
                            temp1.setAvailability(false);
                            temp1.setAvailableAmount(0);
                            System.out.println("[Worker-" + workerId + "] Made product: '"+product+"' un-available successfully");
                        } else {
                            System.err.println("Product already un-available");

                        }

                        break;
                    }

                }

                if (!found) {

                    System.err.println("Product does not exist");

                }

            } else {

                System.err.println("Store does not exist");

            }

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

                List<Product> products;
                products = store.getProducts();

                for (Product temp1 : products) {

                    if (temp1.getProductName().equals(productName)) {

                        found = true;
                        System.err.println("Product: "+productName+" already exists");

                        break;
                    }

                }

                if (!found) {

                    Product pr = new Product(productName,productType,quantity,price);
                    List<Product> prs = store.getProducts();
                    prs.add(pr);
                    store.setProducts(prs);

                    System.out.println("[Worker-" + workerId + "] Added product: '"+productName+"' to store: '"+storeName+"' successfully");

                }

            } else {

                System.err.println("Store does not exist");

            }

        } else if (action.equalsIgnoreCase("remove_old_product")){

            opt = (String) obj;
            String[] parts = opt.split("_", 2);
            String storeName = parts[0];
            String productName = parts.length > 1 ? parts[1] : "";

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                boolean found = false;

                List<Product> products;
                products = store.getProducts();

                for (Product temp1 : products) {

                    if (temp1.getProductName().equals(productName)) {

                        found = true;

                        temp1.setClientAvailability(false);
                        store.setProducts(products);

                        System.out.println("[Worker-" + workerId + "] Removed product: '"+productName+"' from store: '"+storeName+"' successfully");

                        break;
                    }

                }

                if (!found) {

                    System.err.println("Product: "+productName+" does not exist");

                }

            } else {

                System.err.println("Store does not exist");

            }

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

            } catch (IOException e) {
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

            String confirmationMsg = "";
            opt = (String) obj;
            String[] parts = opt.split("_", 5);

            String lon = parts[0];
            double longitude = Double.parseDouble(lon);

            String lat = parts[1];
            double latitude =Double.parseDouble(lat);

            String storeName =  parts[2];

            String productName = parts[3];

            if (storeMap.containsKey(storeName.toLowerCase())) {

                Store store = storeMap.get(storeName.toLowerCase());
                double distance = GeoUtils.haversine(latitude, longitude, store.getLatitude(), store.getLongitude());
                boolean found = false;

                if (distance <= 5.0) {

                    List<Product> products;
                    products = store.getProducts();

                    for (Product product : products) {

                        if (product.getProductName().equals(productName)) {

                            int amount = product.getAvailableAmount();
                            found = true;

                            if (!(amount <= 1)) {
                                product.setAvailableAmount(amount - 1);
                            } else if (amount == 1) {
                                product.setAvailableAmount(0);
                            } else {
                                confirmationMsg = product.getProductName() + " is not available to purchase.";
                                break;
                            }

                            product.setProductSales(1);
                            store.setProducts(products);
                            confirmationMsg = product.getProductName() + " has been purchased.";

                            break;

                        }

                    }

                    if (!found) {
                        confirmationMsg = "Product: " + productName + " does not exist";
                    }

                } else {
                    confirmationMsg = "Store out of range.";
                }

            } else {
                confirmationMsg = "Store does not exist.";
            }

            try {

                Socket socketToReducer = new Socket(host, port);
                ObjectOutputStream objOutToReducer = new ObjectOutputStream(socketToReducer.getOutputStream());

                ActionWrapper wToReducer = new ActionWrapper(confirmationMsg, "confirmation_from_worker", wrapper.getJobID());

                objOutToReducer.writeObject(wToReducer);
                objOutToReducer.flush();
                socketToReducer.close();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else if (action.equalsIgnoreCase("rate_store")) {

            String confirmationMsg;
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

                    store.addStarRating(stars);
                    confirmationMsg = "You gave " + stars + " stars to " + storeName + "!";

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
                socketToReducer.close();

            } catch (IOException e) {
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
                ActionWrapper w = new ActionWrapper(mapped, "mapped_store_results2", ((ActionWrapper) received).getJobID());
                objOut.writeObject(w);
                objOut.flush();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

    }

}

