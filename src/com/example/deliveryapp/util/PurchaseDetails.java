package com.example.deliveryapp.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import java.io.Serializable;
import java.util.Map;

public class PurchaseDetails implements Serializable {

    private static final long serialVersionUID = 1L;

    private double longitude;
    private double latitude;
    private String storeName;
    private Map<String, Integer> productsToPurchase;

    public PurchaseDetails(double longitude, double latitude, String storeName, Map<String, Integer> productsToPurchase) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.storeName = storeName;
        this.productsToPurchase = productsToPurchase;
    }

    public double getLongitude() { return longitude; }

    public double getLatitude() { return latitude; }

    public String getStoreName() { return storeName; }

    public Map<String, Integer> getProductsToPurchase() { return productsToPurchase; }

    @Override
    public String toString() {
        return "PurchaseDetails{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                ", storeName='" + storeName + '\'' +
                ", productsToPurchase=" + productsToPurchase +
                '}';
    }

}