package com.master;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.util.Config;
import com.util.Server;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Master {

    public static void main(String[] args) {

        ObjectOutputStream objOut = null;
        ObjectInputStream objIn = null;

        Object lock = new Object();

        Server server = new Server("Master", 5000, lock);

        Thread thread = new Thread(server);
        thread.start();

        synchronized (Config.configLock) {
            Config.masterReady = true;
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
        }).start();

    }

}



