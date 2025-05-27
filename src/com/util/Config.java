package com.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

public class Config {

    public static volatile int numberOfWorkers = 3;
    public static final Object configLock = new Object();
    public static final Object menuLock = new Object();

    public static volatile boolean workersReady = false;
    public static volatile boolean reducerReady = false;
    public static volatile boolean masterReady = false;
    public static volatile boolean actionFinished = false;

    public static synchronized void setNumberOfWorkers(int num) {
        Config.numberOfWorkers = num;
    }

    public static synchronized int getNumberOfWorkers() { return Config.numberOfWorkers; }

}