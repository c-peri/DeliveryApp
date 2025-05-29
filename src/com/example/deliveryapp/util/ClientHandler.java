package com.example.deliveryapp.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.example.deliveryapp.master.ActionsForMaster;
import com.example.deliveryapp.reducer.ActionsForReducer;
import com.example.deliveryapp.worker.ActionsForWorkers;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class ClientHandler implements Runnable {

    private static final List<AbstractMap.SimpleEntry<String, Store>> allMappedResults = Collections.synchronizedList(new ArrayList<>());
    private static volatile int workersResponded = 0;
    private static final Object reducerLock = new Object();
    private final Socket socket;
    private HashMap<String,Store> hashMap;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    private static final Map<String, List<Store>> resultsMap = Collections.synchronizedMap(new HashMap<>());
    private static final Object resultsLock = new Object();
    private static final Map<String, Object> jobMonitors = Collections.synchronizedMap(new HashMap<>());

    public ClientHandler(Socket socket) { this.socket = socket; }

    public ClientHandler(Socket socket, HashMap<String,Store> hashMap) {
        this.socket = socket;
        this.hashMap = hashMap;
    }

    public static void addFinalResults(List<Store> results, String jobID) {
        synchronized (resultsLock) {
            resultsMap.put(jobID, results);
            System.out.println("Printing jobID: " + resultsMap.keySet());
            System.out.println("Current keys in map: " + resultsMap.size());

            Object monitor = jobMonitors.get(jobID);
            if (monitor != null) {
                synchronized (monitor) {
                    monitor.notifyAll();
                }
            }
        }
    }

    public static void addMappedResults(List<AbstractMap.SimpleEntry<String, Store>> resultList, String jobID, String action) {

        synchronized (reducerLock) {

            System.out.println("Starting map, workers responded: " + workersResponded);
            System.out.println("JobID: " + jobID);
            System.out.println("ResultList: " + resultList);

            int currentExpectedWorkers = Config.getNumberOfWorkers();

            if (!resultList.isEmpty()) {
                allMappedResults.addAll(resultList);
            } else {
                System.out.println("Searching stores...");
            }

            workersResponded++;

            if (workersResponded >= currentExpectedWorkers) {

                System.out.println("Search complete.");

                List<AbstractMap.SimpleEntry<String, Store>> resultsToProcess = new ArrayList<>(allMappedResults);
                allMappedResults.clear();
                System.out.println("workers responded: " + workersResponded);
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

                new Thread(new ActionsForReducer("localhost", 5000, action, reducedList, jobID)).start();

            }

        }

    }

    public void run() {

        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        String jobID, opt, input = "";

        while (true) {

            try {
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());

                Object received = in.readObject();

                ActionWrapper wrapper = (ActionWrapper) received;
                Object obj = wrapper.getObject();
                String action = wrapper.getAction();
                jobID = wrapper.getJobID();
                System.out.println("JobID: " + jobID);


                int localPort = socket.getLocalPort();
                int numOfWorkers = Config.getNumberOfWorkers();

                switch (localPort) {

                    case 5000:

                        System.out.println(socket.getPort());

                        if (action.startsWith("mapped_store_results")) {

                            if (action.equalsIgnoreCase("mapped_store_results")) {

                                List<Store> finalResults = (List<Store>) wrapper.getObject();
                                String receivedJobID = wrapper.getJobID();

                                System.out.println(receivedJobID);
                                System.out.println(finalResults);

                                addFinalResults(finalResults, receivedJobID);
//                                System.out.println("Flushing to: " + socket.getRemoteSocketAddress());
//                                ActionWrapper responseToClient = new ActionWrapper(finalResults, "final_results", receivedJobID);
//                                out.writeObject(responseToClient);
//                                out.flush();

                                return;

                            } else if (action.equalsIgnoreCase("mapped_store_results1")) {

                                List<Store> finalResults = (List<Store>) wrapper.getObject();
                                String receivedJobID = wrapper.getJobID();
                                int total = 0;

                                System.out.println(receivedJobID);

                                for (Store s : finalResults) {
                                    s.setStoreSales();
                                    System.out.println(s.getStoreName() + " : " + s.getStoreSales());
                                    total += s.getStoreSales();
                                }

                                System.out.println("Total : " + total);

                                return;

                            } else {

                                String[] parts1 = action.split("_", 4);

                                List<Store> finalResults = (List<Store>) wrapper.getObject();
                                String receivedJobID = wrapper.getJobID();
                                int total = 0;

                                System.out.println(receivedJobID);

                                for (Store s : finalResults) {
                                    for (Product p : s.getProducts()) {
                                        if (p.getProductType().equalsIgnoreCase(parts1[3])) {
                                            System.out.println(s.getStoreName() + " : " + p.getProductSales());
                                            total += p.getProductSales();
                                        }

                                    }
                                }

                                System.out.println("Total : " + total);

                                return;

                            }

                        } else if (action.equalsIgnoreCase("json") || // Manager actions
                                action.equalsIgnoreCase("add_available_product") ||
                                action.equalsIgnoreCase("remove_available_product") ||
                                action.equalsIgnoreCase("add_new_product") ||
                                action.equalsIgnoreCase("remove_old_product")) {


                            if (action.equalsIgnoreCase("json")) {

                                Store store = (Store) obj;

                                if (store != null) {

                                    int workerId = HashStore.getWorkerID(store.getStoreName(), numOfWorkers);
                                    int workerPort = 5001 + workerId;

                                    new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                                } else {

                                    System.out.println("[Handler]->[Master] Message received: " + input);

                                }


                            } else if (action.equalsIgnoreCase("add_available_product")) {

                                opt = (String) obj;
                                String[] parts;
                                String storeName;

                                parts = opt.split("_", 3);

                                storeName = parts[0];
                                int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                                int workerPort = 5001 + workerId;

                                new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                            } else if (action.equalsIgnoreCase("remove_available_product")) {

                                opt = (String) obj;
                                String[] parts;
                                String storeName;

                                parts = opt.split("_", 2);

                                storeName = parts[0];
                                int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                                int workerPort = 5001 + workerId;

                                new Thread(new ActionsForMaster("localhost", workerPort, wrapper, workerId)).start();

                            } else if (action.equalsIgnoreCase("add_new_product")) {

                                opt = (String) obj;
                                String[] parts;
                                String storeName;

                                parts = opt.split("_", 5);

                                storeName = parts[0];
                                int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
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
                            }


                        } else if (action.equalsIgnoreCase("showcase_stores") || action.equalsIgnoreCase("search_food_preference") ||
                                action.equalsIgnoreCase("search_ratings") || action.equalsIgnoreCase("search_price_range") ||
                                action.equalsIgnoreCase("purchase_product") || action.equalsIgnoreCase("rate_store")) {

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

                            } else if (action.equalsIgnoreCase("showcase_stores") || action.equalsIgnoreCase("search_food_preference") ||
                                    action.equalsIgnoreCase("search_ratings") || action.equalsIgnoreCase("search_price_range")) {

                                for (int i = 1; i <= numOfWorkers; i++) {
                                    ActionWrapper clonedWrapper = new ActionWrapper(obj, action, jobID);
                                    new Thread(new ActionsForMaster("localhost", 5001 + i, clonedWrapper, i)).start();
                                }

                            }

                            List<Store> clientResults = null;


                            synchronized (resultsLock) {

                                while (!resultsMap.containsKey(jobID)) {

                                    try {

                                        System.out.println("Waiting for results...");

                                        resultsLock.wait(500);

                                    } catch (InterruptedException e) {

                                        Thread.currentThread().interrupt();
                                        System.err.println("Interrupted while waiting for results...");

                                        break;

                                    }

                                }

                                List<Store> resObject = resultsMap.remove(jobID);
                                clientResults = resObject;
                                System.out.println("Printing " + clientResults);

                            }

                            ActionWrapper responseToClient = new ActionWrapper(ServerDataLoader.populateStoreLogosForClient(clientResults), "final_results", jobID);
                            out.writeObject(responseToClient);
                            out.flush();

                            return;

                        } else if (action.equalsIgnoreCase("total_sales_store") || action.equalsIgnoreCase("total_sales_product")) {

                            for (int i = 1; i <= numOfWorkers; i++) {
                                ActionWrapper clonedWrapper = new ActionWrapper(obj, action, jobID);
                                new Thread(new ActionsForMaster("localhost", 5001 + i, clonedWrapper, i)).start();
                            }

                        }

                        break;


                    case 5001:

                        System.out.println(socket.getPort());

                        if (action.startsWith("mapped_store_results") || action.equalsIgnoreCase("confirmation_from_worker")) {

                            List<AbstractMap.SimpleEntry<String, Store>> resultList = null;
                            String confirmFromWorker = null;

                            if (obj instanceof List<?>) {
                                resultList = (List<AbstractMap.SimpleEntry<String, Store>>) obj;
                            } else if (obj instanceof String) {
                                confirmFromWorker = (String) obj;
                            }

                            if (resultList != null) {
                                addMappedResults(resultList, wrapper.getJobID(), action);
                            } else if (confirmFromWorker != null) {
                                new Thread(new ActionsForReducer("localhost", 5000, action, confirmFromWorker, wrapper.getJobID())).start();
                            }

                            return;

                        }

                        break;

                    default:

                        System.out.println(socket.getPort());

                        if (action.equalsIgnoreCase("json")) {

                            Store store = (Store) obj;

                            if (store != null) {

                                int workerId = HashStore.getWorkerID(store.getStoreName(), numOfWorkers);

                                new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                            } else {
                                System.out.println("[Handler] Message received: " + input);
                            }

                        } else if (action.equalsIgnoreCase("add_available_product")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 3);
                            String storeName = parts[0];

                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("remove_available_product")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 2);
                            String storeName = parts[0];

                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("add_new_product")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 5);
                            String storeName = parts[0];

                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("remove_old_product")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 2);
                            String storeName = parts[0];

                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("showcase_stores")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 3);

                            int workerId = Integer.parseInt(parts[2]);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("search_food_preference")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 4);

                            int workerId = Integer.parseInt(parts[3]);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("search_ratings")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 4);

                            int workerId = Integer.parseInt(parts[3]);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("search_price_range")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 4);

                            int workerId = Integer.parseInt(parts[3]);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("purchase_product")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 5);

                            int workerId = Integer.parseInt(parts[4]);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("rate_store")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 5);

                            int workerId = Integer.parseInt(parts[4]);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        } else if (action.equalsIgnoreCase("total_sales_store") || action.equalsIgnoreCase("total_sales_product")) {

                            opt = (String) obj;
                            String[] parts = opt.split("_", 2);

                            int workerId = Integer.parseInt(parts[1]);

                            new Thread(new ActionsForWorkers("localhost", 5001, wrapper, hashMap, workerId)).start();

                        }

                        break;
                }


            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                System.err.println("[Handler] Error: " + e.getMessage());
                e.printStackTrace();
                try {
                    if (in != null) in.close();
                    if (out != null) out.close();
                    socket.close();
                } catch (IOException closeEx) {
                    System.err.println("Error closing resources: " + closeEx.getMessage());
                }
                break;
            }

            break;


        }
    }

}
