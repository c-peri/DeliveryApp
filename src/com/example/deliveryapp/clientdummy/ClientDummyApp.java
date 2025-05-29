package com.example.deliveryapp.clientdummy;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.example.deliveryapp.util.Client;

import java.util.Scanner;

public class ClientDummyApp {

    public static void main(String[] args) throws InterruptedException {

        Scanner in = new Scanner(System.in);
        String choice;
        boolean pass, con;
        con = true;
        Thread clientThread;

        while (con) {

            pass = false;

            // Printing the menu of options for the client
            while (!pass) {

                System.out.println("Please choose one of the following options:");
                System.out.println("1. Showcase stores within 5 miles radius");
                System.out.println("2. Search stores");
                System.out.println("3. Buy product");
                System.out.println("4. Rate store");
                System.out.println("5. Exit");
                System.out.print("> ");
                choice = in.nextLine();

                switch (choice) {

                    case "1":

                        System.out.println("---------------------------------------------------------------------------------");

                        clientThread = new Thread(new Client("localhost", 5000, "null::showcase_stores", "Client"));
                        clientThread.start();
                        try {
                            clientThread.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        pass = true;

                        break;
                    case "2":

                        System.out.println("---------------------------------------------------------------------------------");

                        String ch;

                        do {
                            System.out.println("Please choose one of the following options:");
                            System.out.println("1. Search stores based on food preference");
                            System.out.println("2. Search stores based on their ratings");
                            System.out.println("3. Search stores based on their price range");
                            System.out.print("> ");
                            ch = in.nextLine();
                        } while (!ch.equalsIgnoreCase("1") && !ch.equalsIgnoreCase("2") && !ch.equalsIgnoreCase("3"));

                        if (ch.equalsIgnoreCase("1")) {

                            System.out.println("---------------------------------------------------------------------------------");

                            clientThread = new Thread(new Client("localhost", 5000, "null::search_food_preference", "Client"));
                            clientThread.start();
                            try {
                                clientThread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        } else if (ch.equalsIgnoreCase("2")) {

                            System.out.println("---------------------------------------------------------------------------------");

                            clientThread = new Thread(new Client("localhost", 5000, "null::search_ratings", "Client"));
                            clientThread.start();
                            try {
                                clientThread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        } else {

                            System.out.println("---------------------------------------------------------------------------------");

                            clientThread = new Thread(new Client("localhost", 5000, "null::search_price_range", "Client"));
                            clientThread.start();
                            try {
                                clientThread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        }

                        pass = true;

                        break;
                    case "3":

                        System.out.println("---------------------------------------------------------------------------------");

                        clientThread = new Thread(new Client("localhost", 5000, "null::purchase_product", "Client"));
                        clientThread.start();
                        try {
                            clientThread.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        pass = true;

                        break;
                    case "4":

                        System.out.println("---------------------------------------------------------------------------------");

                        clientThread = new Thread(new Client("localhost", 5000, "null::rate_store", "Client"));
                        clientThread.start();
                        try {
                            clientThread.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        pass = true;

                        break;
                    case "5":

                        System.out.println("---------------------------------------------------------------------------------");

                        System.out.println("Returning to launcher...");

                        return;
                    default:
                        System.out.println("Error: Please choose one of the options listed");

                }

            }

            do {

                System.out.println("---------------------------------------------------------------------------------");
                System.out.println("Do you wish to continue with the insertion?");
                System.out.print("> ");
                choice = in.nextLine();
                System.out.println("---------------------------------------------------------------------------------");

                if (choice.equalsIgnoreCase("no")) {
                    con = false;
                    break;
                }

            } while (!choice.equalsIgnoreCase("no") && !choice.equalsIgnoreCase("yes"));

        }

        System.out.println("Returning to launcher...");

    }

}
