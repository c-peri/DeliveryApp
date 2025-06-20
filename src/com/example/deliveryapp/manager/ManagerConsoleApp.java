package com.example.deliveryapp.manager;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.example.deliveryapp.util.Client;

import java.util.Scanner;

import static com.example.deliveryapp.util.IPConfig.IP_ADDRESS;

public class ManagerConsoleApp {

    public static void main(String[] args) {

        Scanner in = new Scanner(System.in);
        String choice;
        boolean pass, con;
        con = true;
        Thread clientThread;

        while (con) {

            pass = false;

            // Printing the menu of options for the manager
            while (!pass) {

                System.out.println("Please choose one of the following options:");
                System.out.println("1. Add new store");
                System.out.println("2. Add new available amount to a product");
                System.out.println("3. Remove availability from a product");
                System.out.println("4. Add new product");
                System.out.println("5. Remove old product");
                System.out.println("6. Show total sales");
                System.out.println("7. Exit");
                System.out.print("> ");
                choice = in.nextLine();

                switch (choice) {

                    case "1":

                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                        clientThread = new Thread(new Client(IP_ADDRESS, 5000, "null::json", "Manager"));
                        clientThread.start();
                        try {
                            clientThread.join();  
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        pass = true;

                        break;
                    case "2":

                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                        clientThread = new Thread(new Client(IP_ADDRESS, 5000, "null::add_available_product", "Manager"));
                        clientThread.start();
                        try {
                            clientThread.join();  
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        pass = true;

                        break;
                    case "3":

                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                        clientThread = new Thread(new Client(IP_ADDRESS, 5000, "null::remove_available_product", "Manager"));
                        clientThread.start();
                        try {
                            clientThread.join();  
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        pass = true;

                        break;
                    case "4":

                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                        clientThread = new Thread(new Client(IP_ADDRESS, 5000, "null::add_new_product", "Manager"));
                        clientThread.start();
                        try {
                            clientThread.join();  
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        pass = true;

                        break;
                    case "5":

                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                        clientThread = new Thread(new Client(IP_ADDRESS, 5000, "null::remove_old_product", "Manager"));
                        clientThread.start();
                        try {
                            clientThread.join();  
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        pass = true;

                        break;
                    case "6":

                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                        String ch;

                        do {
                            System.out.println("Please choose one of the following options:");
                            System.out.println("1. Show total sales per store type");
                            System.out.println("2. Show total sales per product category");
                            System.out.print("> ");
                            ch = in.nextLine();
                        } while (!ch.equalsIgnoreCase("1") && !ch.equalsIgnoreCase("2"));

                        if (ch.equalsIgnoreCase("1")) {

                            System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                            clientThread = new Thread(new Client(IP_ADDRESS, 5000, "null::total_sales_store", "Manager"));
                            clientThread.start();
                            try {
                                clientThread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        } else if (ch.equalsIgnoreCase("2")) {

                            System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                            clientThread = new Thread(new Client(IP_ADDRESS, 5000, "null::total_sales_product", "Manager"));
                            clientThread.start();
                            try {
                                clientThread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        }

                        pass = true;

                        break;
                    case "7":

                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                        return;
                    default:

                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");
                        System.out.println("Error: Please choose one of the options listed");
                        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                }

            }

            do {

                System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");
                System.out.println("Do you wish to continue with the insertion?");
                System.out.print("> ");
                choice = in.nextLine();
                System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------------------");

                if (choice.equalsIgnoreCase("no")) {
                    con = false;
                    break;
                }

                if (!choice.equalsIgnoreCase("no") && !choice.equalsIgnoreCase("yes")){
                    System.out.println("Invalid input, please try again. (Yes/No)");
                }

            } while (!choice.equalsIgnoreCase("no") && !choice.equalsIgnoreCase("yes"));

        }

    }

}