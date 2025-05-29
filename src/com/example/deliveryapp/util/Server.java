package com.example.deliveryapp.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Server implements Runnable {

    private int port;
    private String name;
    private Object lock;
    private HashMap<String,Store> hashMap;

    public Server(String name, int port, Object lock) {
        this.name = name;
        this.port = port;
        this.lock = lock;
        this.hashMap = new HashMap<>(16);
        Store invalidStore = new Store("invalid", 0, 0, "invalid", 0, 0, "invalid", null);
        this.hashMap.put("invalidKey", invalidStore);
    }

    public Server(String name, int port, Object lock, HashMap<String,Store> hashMap) {
        this.name = name;
        this.port = port;
        this.lock = lock;
        this.hashMap = hashMap;
    }

    public void run() {

        try {

            ServerSocket serverSocket = new ServerSocket(port);

            synchronized (lock) {
                lock.notifyAll();
            }

            while (true) {

                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from " + clientSocket.getRemoteSocketAddress());

                if (hashMap.containsKey("invalidKey") && hashMap.containsValue(new Store("invalid", 0, 0, "invalid", 0, 0, "invalid", null))){
                    new Thread(new ClientHandler(clientSocket)).start();
                } else {
                    new Thread(new ClientHandler(clientSocket,hashMap)).start();
                }

            }

        } catch (IOException e) {
            throw new RuntimeException("Server " + name + " failed to start on port " + port, e);
        }

    }

}
