package com.example.deliveryapp.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.google.gson.annotations.Expose;

import java.io.Serial;
import java.io.Serializable;

public class Product implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @Expose
    private String ProductName;
    @Expose
    private String ProductType;
    @Expose
    private int AvailableAmount;
    @Expose
    private double Price;
    private boolean availability;
    private boolean client_availability;
    private int productSales = 0;

    public Product(String ProductName, String ProductType, int AvailableAmount, double Price) {
        this.ProductName = ProductName;
        this.ProductType = ProductType;
        this.AvailableAmount = AvailableAmount;
        this.Price = Price;
        this.client_availability = true;
    }

    public String getProductName() {
        return ProductName;
    }

    public void setProductName(String ProductName) {
        this.ProductName = ProductName;
    }

    public String getProductType() {
        return ProductType;
    }

    public void setProductType(String ProductType) {
        this.ProductType = ProductType;
    }

    public int getAvailableAmount() {
        return AvailableAmount;
    }

    public void setAvailableAmount(int AvailableAmount) {
        this.AvailableAmount = AvailableAmount;
        if (this.AvailableAmount == 0){
            this.availability = false;
        }
    }

    public double getPrice() {
        return Price;
    }

    public void setPrice(double Price) {
        this.Price = Price;
    }

    public void setAvailability(boolean availability) { this.availability=availability;}

    public boolean isAvailable() { return availability; }

    public void setClientAvailability(boolean client_availability) { this.client_availability = client_availability; }

    public boolean getClientAvailability() { return this.client_availability; }

    public void setProductSales(int productSales) { this.productSales += productSales; }

    public double getProductSalesMoney() { return this.productSales * this.Price; }

    public int getProductSales() { return this.productSales; }

    @Override
    public String toString() {
        return "ProductName = '" + ProductName + '\'' +
                ", ProductType = '" + ProductType + '\'' +
                ", AvailableAmount = " + AvailableAmount +
                ", Price = " + Price +
                '}';
    }

}
