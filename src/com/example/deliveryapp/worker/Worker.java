package com.example.deliveryapp.worker;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.example.deliveryapp.util.Config;
import com.example.deliveryapp.util.Server;
import com.example.deliveryapp.util.Store;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

public class Worker {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("<WorkerID>)");
        }

        int workerId = Integer.parseInt(args[0]);

        ObjectOutputStream objOut;
        ObjectInputStream objIn = null;

        HashMap<String, Store> StoreMap = new HashMap<>();

        new Thread(() -> {

            try {

                int port = 5002 + workerId - 1;

                Server server = new Server("Worker-" + workerId, port, new Object(), StoreMap);
                new Thread(server).start();

            } catch (Exception e) {
                e.printStackTrace();
            }

        }).start();

        synchronized (Config.configLock) {
            Config.workersReady = true;
            Config.configLock.notifyAll();
        }

    }

}