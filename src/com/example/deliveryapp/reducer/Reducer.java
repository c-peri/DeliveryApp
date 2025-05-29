package com.example.deliveryapp.reducer;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.example.deliveryapp.util.Client;
import com.example.deliveryapp.util.Config;
import com.example.deliveryapp.util.Server;

public class Reducer {

    public static void main(String[] args) {

        Object lock = new Object();

        new Thread(new Server("Reducer", 5001,lock)).start();

        synchronized (Config.configLock) {
            Config.reducerReady = true;
            Config.configLock.notifyAll();
        }

        new Thread(() -> {

            synchronized (lock) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            synchronized (Config.configLock) {
                while (!Config.masterReady || !Config.workersReady) {
                    try {
                        Config.configLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            new Client("localhost", 5000, "Hello from Reducer to Master::null","Reducer").run();
        }).start();

    }

}