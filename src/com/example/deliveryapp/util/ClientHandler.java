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

import static com.example.deliveryapp.util.IPConfig.IP_ADDRESS;

public class ClientHandler implements Runnable {

    private static final List<AbstractMap.SimpleEntry<String, Store>> allMappedResults = Collections.synchronizedList(new ArrayList<>());
    private static volatile int workersResponded = 0;
    private static final Object reducerLock = new Object();
    private final Socket socket;
    private Map<String, Store> hashMap = Collections.synchronizedMap(new HashMap<>());
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

    public ClientHandler(Socket socket, Map <String, Store> hashMap) {
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

            System.out.println("[Reducer] Starting mapping process, workers responded: " + workersResponded);
            System.out.println("[Reducer] Mapping for JobID: " + jobID);
            System.out.println("[Reducer] ResultList: " + resultList);

            int currentExpectedWorkers = Config.getNumberOfWorkers();

            if (!resultList.isEmpty()) {
                allMappedResults.addAll(resultList);
            } else {
                System.out.println("[Reducer] Searching stores...");
            }

            workersResponded++;

            if (workersResponded >= currentExpectedWorkers) {

                System.out.println("[Reducer] Workers responded: " + workersResponded);

                System.out.println("[Reducer] Search complete.");

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
                } else if (grouped.containsKey("sales_store")) {
                    reducedList.addAll(reducer.reduce("sales_store", grouped.get("sales_store")));
                } else if (grouped.containsKey("sales_product")) {
                    reducedList.addAll(reducer.reduce("sales_product", grouped.get("sales_product")));
                }

                Thread thread = new Thread(new ActionsForReducer(IP_ADDRESS, 5000, action, reducedList, jobID));
                thread.start();

            }

        }

    }

    public void run() {

        String jobID;
        String opt;

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

                    if (action.startsWith("mapped_store_results")) {

                        if (action.equalsIgnoreCase("mapped_store_results")) {

                            List<Store> finalResults = (List<Store>) wrapper.getObject();
                            String receivedJobID = wrapper.getJobID();

                            addFinalResults(finalResults, receivedJobID);

                            synchronized (lock) {
                                JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.SEARCH_DONE);
                                lock.notifyAll();
                            }

                            return;

                        } else if (action.equalsIgnoreCase("mapped_store_results1")) {

                            List<Store> finalResults = (List<Store>) wrapper.getObject();
                            String receivedJobID = wrapper.getJobID();

                            System.out.println("[Master] Sending sales statistics to Manager for JobID: " + receivedJobID);

                            addFinalResults(finalResults, receivedJobID);

                            synchronized (lock) {
                                JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.SEARCH_DONE);
                                lock.notifyAll();
                            }

                        } else {

                            List<Store> finalResults = (List<Store>) wrapper.getObject();
                            String receivedJobID = wrapper.getJobID();

                            System.out.println("[Master] Sending sales statistics to Manager for JobID: " + receivedJobID);

                            addFinalResults(finalResults, receivedJobID);

                            synchronized (lock) {
                                JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.SEARCH_DONE);
                                lock.notifyAll();
                            }

                        }

                    } else if (action.equalsIgnoreCase("json") || action.equalsIgnoreCase("add_available_product") ||
                               action.equalsIgnoreCase("remove_available_product") || action.equalsIgnoreCase("add_new_product") ||
                               action.equalsIgnoreCase("remove_old_product") ||
                               action.equalsIgnoreCase("total_sales_store") ||
                               action.equalsIgnoreCase("total_sales_product")) {

                        if (action.equalsIgnoreCase("json")) {

                            Store store = (Store) obj;

                            if (store != null) {

                                int workerId = HashStore.getWorkerID(store.getStoreName(), numOfWorkers);
                                int workerPort = 5001 + workerId;

                                if (workerId == 2){
                                    new Thread(new ActionsForMaster(IP_ADDRESS, workerPort, wrapper, workerId)).start();
                                } else {
                                    new Thread(new ActionsForMaster("172.20.10.3", workerPort, wrapper, workerId)).start();
                                }

                            }

                        } else if (action.equalsIgnoreCase("add_available_product")) {

                            opt = (String) obj;
                            String[] parts;
                            String storeName;

                            parts = opt.split("_", 3);

                            storeName = parts[0];
                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                            int workerPort = 5001 + workerId;

                            if (workerId == 2){
                                new Thread(new ActionsForMaster(IP_ADDRESS, workerPort, wrapper, workerId)).start();
                            } else {
                                new Thread(new ActionsForMaster("172.20.10.3", workerPort, wrapper, workerId)).start();
                            }

                        } else if (action.equalsIgnoreCase("remove_available_product")) {

                            opt = (String) obj;
                            String[] parts;
                            String storeName;

                            parts = opt.split("_", 2);

                            storeName = parts[0];
                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                            int workerPort = 5001 + workerId;

                            if (workerId == 2){
                                new Thread(new ActionsForMaster(IP_ADDRESS, workerPort, wrapper, workerId)).start();
                            } else {
                                new Thread(new ActionsForMaster("172.20.10.3", workerPort, wrapper, workerId)).start();
                            }

                        } else if (action.equalsIgnoreCase("add_new_product")) {

                            opt = (String) obj;
                            String[] parts;
                            String storeName;

                            parts = opt.split("_", 5);

                            storeName = parts[0];
                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                            int workerPort = 5001 + workerId;

                            if (workerId == 2){
                                new Thread(new ActionsForMaster(IP_ADDRESS, workerPort, wrapper, workerId)).start();
                            } else {
                                new Thread(new ActionsForMaster("172.20.10.3", workerPort, wrapper, workerId)).start();
                            }

                        } else if (action.equalsIgnoreCase("remove_old_product")) {

                            opt = (String) obj;
                            String[] parts;
                            String storeName;

                            parts = opt.split("_", 2);

                            storeName = parts[0];
                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                            int workerPort = 5001 + workerId;

                            if (workerId == 2){
                                new Thread(new ActionsForMaster(IP_ADDRESS, workerPort, wrapper, workerId)).start();
                            } else {
                                new Thread(new ActionsForMaster("172.20.10.3", workerPort, wrapper, workerId)).start();
                            }

                        } else if (action.equalsIgnoreCase("total_sales_store") || action.equalsIgnoreCase("total_sales_product")) {

                            for (int i = 1; i <= numOfWorkers; i++) {
                                ActionWrapper clonedWrapper = new ActionWrapper(obj, action, jobID);
                                if (i == 2){
                                    new Thread(new ActionsForMaster(IP_ADDRESS, 5001 + i, clonedWrapper, i)).start();
                                } else {
                                    new Thread(new ActionsForMaster("172.20.10.3", 5001 + i, clonedWrapper, i)).start();
                                }

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

                        System.out.println("[Master] Sending results for JobID: " + jobID);

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

                            if (obj instanceof PurchaseDetails purchaseDetails) {

                                String storeName = purchaseDetails.getStoreName();
                                System.out.println("[Master] Received purchase request for store: " + storeName);

                                int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                                int workerPort = 5001 + workerId;
                                if (workerId == 2){
                                    new Thread(new ActionsForMaster(IP_ADDRESS, workerPort, wrapper, workerId)).start();
                                } else {
                                    new Thread(new ActionsForMaster("172.20.10.3", workerPort, wrapper, workerId)).start();
                                }

                            } else {

                                System.err.println("[Master] Error: obj for purchase_product is not PurchaseDetails. Type: " + (obj != null ? obj.getClass().getName() : "null"));

                                ActionWrapper errorResponse = new ActionWrapper("Invalid purchase data format.", "error", jobID);
                                out.writeObject(errorResponse);
                                out.flush();

                                synchronized (lock) {
                                    JobCoordinator.setStatus(UUID.fromString(jobID), JobCoordinator.JobStatus.FAILED);
                                    lock.notifyAll();
                                }

                                return;

                            }

                        } else if (action.equalsIgnoreCase("rate_store")) {

                            opt = (String) obj;
                            String[] parts;
                            String storeName;

                            parts = opt.split("_", 5);

                            storeName = parts[2];
                            int workerId = HashStore.getWorkerID(storeName, numOfWorkers);
                            int workerPort = 5001 + workerId;

                            if (workerId == 2){
                                new Thread(new ActionsForMaster(IP_ADDRESS, workerPort, wrapper, workerId)).start();
                            } else {
                                new Thread(new ActionsForMaster("172.20.10.3", workerPort, wrapper, workerId)).start();
                            }

                        } else if (action.equalsIgnoreCase("showcase_stores") || action.equalsIgnoreCase("search_food_preference") ||
                                   action.equalsIgnoreCase("search_ratings") || action.equalsIgnoreCase("search_price_range")) {

                            for (int i = 1; i <= numOfWorkers; i++) {
                                ActionWrapper clonedWrapper = new ActionWrapper(obj, action, jobID);
                                if (i==2){
                                    new Thread(new ActionsForMaster(IP_ADDRESS, 5001 + i, clonedWrapper, i)).start();
                                } else {
                                    new Thread(new ActionsForMaster("172.20.10.3", 5001 + i, clonedWrapper, i)).start();
                                }
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

                        System.out.println("[Master] Sending results for JobID: " + jobID);

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

                    } else if (action.equalsIgnoreCase("confirmation_from_worker")) {

                        System.out.println("[Master] Master received 'confirmation_from_worker' for JobID: " + jobID + " from Reducer (port " + socket.getPort() + ").");
                        String confirmationMessageFromWorker = (String) wrapper.getObject();
                        System.out.println("[Master] Confirmation message content from Reducer: '" + confirmationMessageFromWorker + "'");

                        JobTracker targetTracker;
                        synchronized (jobTrackersLock) {
                            targetTracker = jobTrackers.get(jobID);
                            if (targetTracker == null) {
                                System.err.println("[Master_ERROR] JobTracker for JobID " + jobID + " NOT FOUND .");

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

                        String confirmationMessageFromWorker = (String) wrapper.getObject();
                        System.out.println("[Master] Confirmation message content: '" + confirmationMessageFromWorker + "'");

                        JobTracker targetTracker;
                        synchronized (jobTrackersLock) {
                            targetTracker = jobTrackers.get(jobID);
                            if (targetTracker == null) {
                                System.err.println("[Master_ERROR] JobTracker for JobID " + jobID + " NOT FOUND for manager confirmation.");
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
                            new Thread(new ActionsForReducer("172.20.10.3", 5000, "confirmation_from_worker", confirmFromWorker, wrapper.getJobID())).start();
                        }

                        break;

                    }

                    break;

                default:

                    if (action.equalsIgnoreCase("json")) {

                        Store store = (Store) obj;

                        if (store != null) {

                            int workerId = HashStore.getWorkerID(store.getStoreName(), numOfWorkers);

                            new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                        }

                    } else if (action.equalsIgnoreCase("add_available_product")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 3);
                        String storeName = parts[0];

                        int workerId = HashStore.getWorkerID(storeName, numOfWorkers);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("remove_available_product")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 2);
                        String storeName = parts[0];

                        int workerId = HashStore.getWorkerID(storeName, numOfWorkers);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("add_new_product")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 5);
                        String storeName = parts[0];

                        int workerId = HashStore.getWorkerID(storeName, numOfWorkers);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("remove_old_product")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 2);
                        String storeName = parts[0];

                        int workerId = HashStore.getWorkerID(storeName, numOfWorkers);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("showcase_stores")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 3);

                        int workerId = Integer.parseInt(parts[2]);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("search_food_preference")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 4);

                        int workerId = Integer.parseInt(parts[3]);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("search_ratings")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 4);

                        int workerId = Integer.parseInt(parts[3]);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("search_price_range")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 4);

                        int workerId = Integer.parseInt(parts[3]);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("purchase_product")) {

                        int workerId = localPort - 5001;

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("rate_store")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 5);

                        int workerId = Integer.parseInt(parts[4]);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    } else if (action.equalsIgnoreCase("total_sales_store") || action.equalsIgnoreCase("total_sales_product")) {

                        opt = (String) obj;
                        String[] parts = opt.split("_", 2);

                        int workerId = Integer.parseInt(parts[1]);

                        new Thread(new ActionsForWorkers("172.20.10.3", 5001, wrapper, hashMap, workerId)).start();

                    }

                    break;
            }

        } catch (ClassNotFoundException e) {
            System.err.println("[ClientHandler] ClassNotFoundException: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("[ClientHandler] IOException (connection closed or error): " + e.getMessage());
        } finally {
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
