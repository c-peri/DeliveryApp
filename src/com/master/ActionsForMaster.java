package com.master;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.util.ActionWrapper;
import com.util.Store;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

public class ActionsForMaster implements Runnable {

    private String host;
    private int port;
    private Object received;
    private int workerID;

    public ActionsForMaster(String host, int port,Object received, int workerID) {
        this.host = host;
        this.port = port;
        this.received = received;
        this.workerID = workerID;
    }

    public void run() {

        ObjectOutputStream objOut;

        try {

            ActionWrapper w = (ActionWrapper) received;

            if (w.getAction().equals( "mapped_store_results")) {

                List<Store> stores = (List<Store>) w.getObject();

                if (stores.isEmpty()){
                    System.out.println("No stores found matching your criteria");
                } else {
                    System.out.println("Found " + stores.size() + " stores");
                    for (Store store : stores) {
                        System.out.println(">" + store.getStoreName());
                    }
                }

                return;

            }

            Socket socket = new Socket(host, port);
            objOut = new ObjectOutputStream(socket.getOutputStream());

            if (w.getAction().equals("sales_food_category")  || w.getAction().equals("sales_product_category") ||
                w.getAction().equals("showcase_stores") || w.getAction().equals("search_food_preference") ||
                w.getAction().equals("search_ratings") ||  w.getAction().equals("search_price_range") ||
                w.getAction().equals("purchase_product") || w.getAction().equals("rate_store") ||
                w.getAction().equals("total_sales_store") || w.getAction().equals("total_sales_product")) {

                w.setObject(String.valueOf(workerID));

                received = w;

            }

            objOut.writeObject(received);
            objOut.flush();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
