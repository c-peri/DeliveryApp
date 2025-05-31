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
    private HashMap<String, Store> hashMap;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    private static final Object resultsLock = new Object();
    private static final Map<String, JobTracker> jobTrackers = new HashMap<>();
    private static final Object jobTrackersLock = new Object();

    private static class JobTracker {
        final Object monitor = new Object();
        Object result = null;
    }


    public ClientHandler(Socket socket) {
        this.socket = socket;
        try {
            in = new ObjectInputStream(socket.getInputStream());
            out = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            System.err.println("Error initializing streams for ClientHandler (socket " + socket.getLocalPort() + "): " + e.getMessage());
            try { socket.close(); } catch (IOException closeEx) { /* ignore */ }
        }
    }

    public ClientHandler(Socket socket, HashMap<String, Store> hashMap) {
        this(socket);
        this.hashMap = hashMap;
    }

    public static void addFinalResults(List<Store> results, String jobID) {
        JobTracker tracker;
        synchronized (jobTrackersLock) {
            tracker = jobTrackers.get(jobID);
            if (tracker == null) {
                tracker = new JobTracker();
                jobTrackers.put(jobID, tracker);
            }
        }

        synchronized (tracker.monitor) {
            tracker.result = results;
            System.out.println("[NOTIFY] Notifying monitor for JobID: " + jobID);
            tracker.monitor.notifyAll();
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

                Thread thread = new Thread(new ActionsForReducer("localhost", 5000, action, reducedList, jobID));
                thread.start();
//                try {
//                    thread.join();
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }


            }

        }

    }

    public void run() {

        String jobID;
        String opt;
        String input = "";




            try {

                Object received = in.readObject();

                ActionWrapper wrapper = (ActionWrapper) received;
                Object obj = wrapper.getObject();
                String action = wrapper.getAction();
                jobID = wrapper.getJobID();
                Object lock = JobCoordinator.getLock(UUID.fromString(jobID));

                System.out.println("JobID: " + jobID);

                int localPort = socket.getLocalPort();
                int numOfWorkers = Config.getNumberOfWorkers();

                switch (localPort) {

                    case 5000:

                        System.out.println(socket.getPort());

                        if (action.startsWith("mapped_store_results")) {

                            if (action.equalsIgnoreCase("mapped_store_results")) {
                                System.out.println(wrapper.getObject());
                                System.out.println(wrapper.getAction());
                                List<Store> finalResults = (List<Store>) wrapper.getObject();
                                String receivedJobID = wrapper.getJobID();

                                System.out.println(receivedJobID);
                                System.out.println(finalResults);

                                addFinalResults(finalResults, receivedJobID);

                                synchronized (lock) {
                                    JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.SEARCH_DONE);
                                    lock.notifyAll();
                                }

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
                            Object clientResults;
                            JobTracker tracker;
                            synchronized (jobTrackersLock) {
                                tracker = jobTrackers.get(jobID);
                                if (tracker == null) {
                                    tracker = new JobTracker();
                                    jobTrackers.put(jobID, tracker);
                                }
                            }

                            synchronized (tracker.monitor) {
                                while (tracker.result == null) {
                                    try {
                                        System.out.println("[WAIT] Waiting on jobID: " + jobID);
                                        tracker.monitor.wait();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        System.err.println("Interrupted while waiting on jobID: " + jobID);
                                        return;
                                    }
                                }

                                clientResults = tracker.result;
                                synchronized (jobTrackersLock) {
                                    jobTrackers.remove(jobID); // Cleanup
                                }
                            }


                            System.out.println("Printing " + clientResults);

                            if (clientResults instanceof List<?>) {
                                ActionWrapper responseToClient = new ActionWrapper(ServerDataLoader.populateStoreLogosForClient((List<Store>) clientResults), "final_results", jobID);
                                out.writeObject(responseToClient);
                                out.flush();
                            } else if (clientResults instanceof String) {
                                ActionWrapper responseToClient = new ActionWrapper(clientResults, "confirmation_message", jobID);
                                out.writeObject(responseToClient);
                                out.flush();
                            }
                            synchronized (lock) {
                                JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.COMPLETED);
                                lock.notifyAll();
                            }
                            return;

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
                            Object clientResults;
                            JobTracker tracker;
                            synchronized (jobTrackersLock) {
                                tracker = jobTrackers.get(jobID);
                                if (tracker == null) {
                                    tracker = new JobTracker();
                                    jobTrackers.put(jobID, tracker);
                                }
                            }

                            synchronized (tracker.monitor) {
                                while (tracker.result == null) {
                                    try {
                                        System.out.println("[WAIT] Waiting on jobID: " + jobID);
                                        tracker.monitor.wait();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        System.err.println("Interrupted while waiting on jobID: " + jobID);
                                        return;
                                    }
                                }

                                clientResults = tracker.result;
                                synchronized (jobTrackersLock) {
                                    jobTrackers.remove(jobID);
                                }
                            }


                            System.out.println("Printing " + clientResults);

                            if (clientResults instanceof List<?>) {
                                ActionWrapper responseToClient = new ActionWrapper(ServerDataLoader.populateStoreLogosForClient((List<Store>) clientResults), "final_results", jobID);
                                out.writeObject(responseToClient);
                                out.flush();
                            } else if (clientResults instanceof String) {
                                ActionWrapper responseToClient = new ActionWrapper(clientResults, "confirmation_message", jobID);
                                out.writeObject(responseToClient);
                                out.flush();
                            }
                            synchronized (lock) {
                                JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.COMPLETED);
                                lock.notifyAll();
                            }
                            return;


                        } else if (action.equalsIgnoreCase("total_sales_store") || action.equalsIgnoreCase("total_sales_product")) {

                            for (int i = 1; i <= numOfWorkers; i++) {
                                ActionWrapper clonedWrapper = new ActionWrapper(obj, action, jobID);
                                new Thread(new ActionsForMaster("localhost", 5001 + i, clonedWrapper, i)).start();
                            }

                        } else if (action.equalsIgnoreCase("confirmation_from_worker")) {
                            System.out.println("[MASTER] Master received 'confirmation_from_worker' for JobID: " + jobID + " from Reducer (port " + socket.getPort() + ").");
                            String confirmationMessageFromWorker = (String) wrapper.getObject();
                            System.out.println("[MASTER] Confirmation message content from Reducer: '" + confirmationMessageFromWorker + "'");

                            JobTracker targetTracker;
                            synchronized (jobTrackersLock) {
                                targetTracker = jobTrackers.get(jobID);
                                if (targetTracker == null) {
                                    System.err.println("[MASTER_CH_ERROR] JobTracker for JobID " + jobID + " NOT FOUND .");

                                    return;
                                }
                            }
                            synchronized (targetTracker.monitor) {

                                targetTracker.result = wrapper.getObject();
                                targetTracker.monitor.notifyAll();
                            }


                            synchronized (lock) {
                                JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.COMPLETED);
                                lock.notifyAll();
                            }
                            return;
                        } else if (action.equalsIgnoreCase("manager_confirmation")) {

                            System.out.println("[MASTER] Master received 'manager_confirmation' for JobID: " + jobID + " from Worker/Confirmation Sender.");
                            String confirmationMessageFromWorker = (String) wrapper.getObject();
                            System.out.println("[MASTER] Confirmation message content: '" + confirmationMessageFromWorker + "'");

                            JobTracker targetTracker;
                            synchronized (jobTrackersLock) {
                                targetTracker = jobTrackers.get(jobID);
                                if (targetTracker == null) {
                                    System.err.println("[MASTER_CH_ERROR] JobTracker for JobID " + jobID + " NOT FOUND for manager confirmation.");
                                    return;
                                }
                            }
                            synchronized (targetTracker.monitor) {
                                targetTracker.result = confirmationMessageFromWorker;
                                targetTracker.monitor.notifyAll();
                            }
                            synchronized (lock) {
                                JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.COMPLETED);
                                lock.notifyAll();
                            }
                            return;
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
                                new Thread(new ActionsForReducer("localhost", 5000, "confirmation_from_worker", confirmFromWorker, wrapper.getJobID())).start();
                            }

                            break;

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
                System.err.println("[ClientHandler] ClassNotFoundException: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("[ClientHandler] IOException (connection closed or error): " + e.getMessage());
                // This is where a client disconnecting or a network error will be caught.
            } finally {
                // Ensure streams and socket are closed when the thread exits
                // This is important because the ClientHandler processes only one request then terminates
                try {
                    if (in != null) in.close();
                    if (out != null) out.close();
                    if (socket != null && !socket.isClosed()) socket.close();
                } catch (IOException closeEx) {
                    System.err.println("Error closing resources in ClientHandler: " + closeEx.getMessage());
                }
                }



    }

}
