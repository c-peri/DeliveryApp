/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana || p3220160@aueb.gr
 *
 */

package com.util;

public class HashStore {

    public HashStore() {}

    public static int getWorkerID(String storeName, int numOfWorkers) {

        int hash = Math.abs(storeName.toLowerCase().hashCode());

        return (hash % numOfWorkers) + 1;

    }

}