package com.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.google.gson.*;
import com.master.ActionsForMaster;
import com.reducer.ActionsForReducer;
import com.worker.ActionsForWorkers;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class ClientHandler implements Runnable {

    private static final List<AbstractMap.SimpleEntry<String, Store>> allMappedResults = Collections.synchronizedList(new ArrayList<>());
    private static volatile int workersResponded = 0;
    private static final Object reducerLock = new Object();
    private Socket socket;
    private HashMap<String,Store> hashMap;

    private static final Map<String, List<Store>> resultsMap = Collections.synchronizedMap(new HashMap<>());
    private static final Object resultsLock = new Object();

    public ClientHandler(Socket socket) { this.socket = socket; }

    public ClientHandler(Socket socket, HashMap<String,Store> hashMap) {
        this.socket = socket;
        this.hashMap = hashMap;
    }

    public static void addFinalResults(List<Store> results, String jobID) {
        synchronized (resultsLock) {
            resultsMap.put(jobID, results);
            System.out.println("Printing jobID: " + resultsMap.get(jobID));
            System.out.println("Current keys in map: " + resultsMap.keySet());
            resultsLock.notifyAll();
        }
    }
    public static void addMappedResults(List<AbstractMap.SimpleEntry<String, Store>> resultList, String jobID) {
        synchronized (reducerLock) {
            System.out.println("JobID: " + jobID);
            System.out.println("ResultList: " + resultList);
            int currentExpectedWorkers = Config.getNumberOfWorkers();

            if (!resultList.isEmpty()) {

                allMappedResults.addAll(resultList);
                workersResponded++;

            } else {
                workersResponded++;
                System.out.println("Searching stores...");

            }

            if (workersResponded >= currentExpectedWorkers) {

                System.out.println("Search complete.");
                List<AbstractMap.SimpleEntry<String, Store>> resultsToProcess = new ArrayList<>(allMappedResults);
                allMappedResults.clear();
                workersResponded = 0;

                Map<String, List<Store>> grouped = new HashMap<>();

                for (AbstractMap.SimpleEntry<String, Store> entry : resultsToProcess) {

                    if (entry.getKey() != null && entry.getValue() != null) {
                        grouped.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
                    } else {
                        System.out.println("[Reducer] Warning: Encountered null key or Store value in mapped results.");
                    }

                }

                StoreReducer reducer = new StoreReducer();
                List<Store> reducedList = new ArrayList<>();

                if (grouped.containsKey("filtered_store")) {
                    reducedList.addAll(reducer.reduce("filtered_store", grouped.get("filtered_store")));
                } else if (grouped.containsKey("within_range")) {
                    reducedList.addAll(reducer.reduce("within_range", grouped.get("within_range")));
                }

                new Thread(new ActionsForReducer("localhost", 5000, reducedList, jobID)).start();

            }

        }

    }

    public void run() {

        Store store;
        String input = "";
        String action,opt;
        Object obj;
        ActionWrapper wrapper;

        while (true){

            try {

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream inObj = new ObjectInputStream(socket.getInputStream());
                Object received = inObj.readObject();

                wrapper = (ActionWrapper) received;
                obj = wrapper.getObject();
                action = wrapper.getAction();
                String jobID = wrapper.getJobID();
                System.out.println("JobID: " + jobID);

                int port = socket.getLocalPort();
                int numOfWorkers = Config.getNumberOfWorkers();

                switch (port){

                    case 5000:
                        System.out.println(socket.getPort());

                        if (action.equalsIgnoreCase("json")){

                            store = (Store) obj;

                            if (store!=null) {

                                int workerId = HashStore.getWorkerID(store.getStoreName(),numOfWorkers);
                                int workerPort = 5001 + workerId;

                                new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                            } else {

                                System.out.println("[Handler]->[Master] Message received: " + input);

                            }

                        } else {

                            if (action.equalsIgnoreCase("add_available_product")){

                                opt = (String) obj;
                                String[] parts;
                                String storeName;

                                parts = opt.split("_", 3);

                                storeName = parts[0];
                                int workerId = HashStore.getWorkerID(storeName,numOfWorkers);
                                int workerPort = 5001 + workerId;

                                new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                            } else if (action.equalsIgnoreCase("remove_available_product")){

                                opt = (String) obj;
                                String[] parts;
                                String storeName;

                                parts = opt.split("_", 2);

                                storeName = parts[0];
                                int workerId = HashStore.getWorkerID(storeName,numOfWorkers);
                                int workerPort = 5001 + workerId;

                                new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                            } else if (action.equalsIgnoreCase("add_new_product")){

                                opt = (String) obj;
                                String[] parts;
                                String storeName;

                                parts = opt.split("_", 5);

                                storeName = parts[0];
                                int workerId = HashStore.getWorkerID(storeName,numOfWorkers);
                                int workerPort = 5001 + workerId;

                                new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                            } else if (action.equalsIgnoreCase("remove_old_product")) {

                                opt = (String) obj;
                                String[] parts;
                                String storeName;

                                parts = opt.split("_", 2);

                                storeName = parts[0];
                                int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                                int workerPort = 5001 + workerId;

                                new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                            } else if (action.equalsIgnoreCase("showcase_stores") ||
                                    action.equalsIgnoreCase("search_food_preference") ||
                                    action.equalsIgnoreCase("search_ratings") ||
                                    action.equalsIgnoreCase("search_price_range") ||
                                    action.equalsIgnoreCase("purchase_product") ||
                                    action.equalsIgnoreCase("rate_store")) {

                                if (action.equalsIgnoreCase("purchase_product")) {

                                    opt = (String) obj;
                                    String[] parts;
                                    String storeName;

                                    parts = opt.split("_", 5);

                                    storeName = parts[2];
                                    int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                                    int workerPort = 5001 + workerId;

                                    new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                                } else if (action.equalsIgnoreCase("rate_store")) {

                                    opt = (String) obj;
                                    String[] parts;
                                    String storeName;

                                    parts = opt.split("_", 5);

                                    storeName = parts[2];
                                    int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                                    int workerPort = 5001 + workerId;

                                    new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                                } else if (action.equalsIgnoreCase("showcase_stores") || action.equalsIgnoreCase("search_food_preference") || action.equalsIgnoreCase("search_ratings") ||
                                        action.equalsIgnoreCase("search_price_range")) {

                                    for (int i = 1; i <= numOfWorkers; i++) {
                                        ActionWrapper clonedWrapper = new ActionWrapper(obj, action, jobID);
                                        new Thread(new ActionsForMaster("localhost", 5001 + i, clonedWrapper, i)).start();
                                    }

                                } else if (action.equalsIgnoreCase("mapped_store_results")) {

                                    List<Store> finalResults = (List<Store>) wrapper.getObject();
                                    addFinalResults(finalResults, wrapper.getJobID());
                                    System.out.println(resultsMap);

                                }

                                List<Store> clientResults = null;
                                String message = null;
                                String error = null;

                                synchronized (resultsLock) {
                                    while (!resultsMap.containsKey(jobID)) {
                                        try {
                                            System.out.println("Waiting for results...");
                                            resultsLock.wait(500);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            System.err.println("Interrupted while waiting for results...");
                                            error = "Request interrupted";
                                            break;
                                        }
                                    }

                                    if (error == null) {
                                        Object resObject = resultsMap.remove(jobID);
                                        if (resObject instanceof List<?>) {
                                            clientResults = (List<Store>) resObject;
                                            System.out.println(clientResults);
                                        } else if (resObject instanceof String) {
                                            message = (String) resObject;
                                            System.out.println(message);
                                        } else {
                                            error = "Unexpected result type";
                                        }
                                    }
                                }

// Decide action type for response
                                String responseAction;
                                Object responseData;

                                if (error != null) {
                                    responseAction = "error";
                                    responseData = error;
                                } else if (clientResults != null) {
                                    responseAction = action; // same action back to client
                                    responseData = clientResults;
                                } else if (message != null) {
                                    switch (action.toLowerCase()) {
                                        case "purchase_product":
                                            responseAction = "purchase_confirmation";
                                            break;
                                        case "rate_store":
                                            responseAction = "rate_confirmation";
                                            break;
                                        default:
                                            responseAction = "generic_confirmation";
                                    }
                                    responseData = message;
                                } else {
                                    responseAction = "error";
                                    responseData = "No data received.";
                                }

// Send response
                                ActionWrapper responseToClient = new ActionWrapper(responseData, responseAction, jobID);
                                out.writeObject(responseToClient);
                                out.flush();
                                return;

                            } else if (action.equalsIgnoreCase("mapped_store_results")) {
                                List<Store> finalResults = (List<Store>) wrapper.getObject();
                                String receivedJobID = wrapper.getJobID();
                                System.out.println(receivedJobID);
                                System.out.println(finalResults);
                                addFinalResults(finalResults, receivedJobID);

                                return;

                            }

                        }

                        break;

                    case 5001:
                        System.out.println(socket.getPort());

                        if (action.equalsIgnoreCase("mapped_store_results") || action.equalsIgnoreCase("confirmation_from_worker")) {

                            List<AbstractMap.SimpleEntry<String, Store>> resultList = null;
                            String confirmFromWorker = null;

                            if (obj instanceof List<?>) {
                                resultList = (List<AbstractMap.SimpleEntry<String, Store>>) obj;
                            } else if (obj instanceof String) {
                                confirmFromWorker = (String) obj;
                            }

                            if (resultList != null) {
                                addMappedResults(resultList, wrapper.getJobID());
                            } else if (confirmFromWorker != null) {
                                new Thread(new ActionsForReducer("localhost", 5000, confirmFromWorker, wrapper.getJobID())).start();
                            }

                            return;

                        }

                        break;

                    default:
                        System.out.println(socket.getPort());

                        if (action.equalsIgnoreCase("json")){

                            store = (Store) obj;

                            if (store != null) {

                                int workerId = HashStore.getWorkerID(store.getStoreName(),numOfWorkers);

                                new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                            } else {

                                System.out.println("[Handler] Message received: " + input);
                            }

                        } else if (action.equalsIgnoreCase("add_available_product")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 3);
                            String storeName = parts[0];

                            int workerId = HashStore.getWorkerID(storeName,numOfWorkers);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        } else if (action.equalsIgnoreCase("remove_available_product")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 2);
                            String storeName = parts[0];

                            int workerId = HashStore.getWorkerID(storeName,numOfWorkers);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        } else if (action.equalsIgnoreCase("add_new_product")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 5);
                            String storeName = parts[0];

                            int workerId = HashStore.getWorkerID(storeName,numOfWorkers);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        } else if (action.equalsIgnoreCase("remove_old_product")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 2);
                            String storeName = parts[0];

                            int workerId = HashStore.getWorkerID(storeName,numOfWorkers);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        } else if (action.equalsIgnoreCase("showcase_stores")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 3);

                            int workerId = Integer.parseInt(parts[2]);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        } else if (action.equalsIgnoreCase("search_food_preference")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 4);

                            int workerId = Integer.parseInt(parts[3]);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();


                        } else if (action.equalsIgnoreCase("search_ratings")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 4);

                            int workerId = Integer.parseInt(parts[3]);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        } else if (action.equalsIgnoreCase("search_price_range")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 4);

                            int workerId = Integer.parseInt(parts[3]);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        } else if (action.equalsIgnoreCase("purchase_product")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 5);

                            int workerId = Integer.parseInt(parts[4]);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        } else if (action.equalsIgnoreCase("rate_store")){

                            opt = (String) obj;
                            String[] parts = opt.split("_", 5);

                            int workerId = Integer.parseInt(parts[4]);

                            new Thread(new ActionsForWorkers("localhost", 5001,wrapper,hashMap,workerId)).start();

                        }

                        break;

                }

                break;

            } catch (JsonSyntaxException e) {
                e.printStackTrace();
            } catch (IOException ignored) {

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

        }

    }

}
