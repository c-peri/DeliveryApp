package com.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.*;

public class Store implements Serializable {

    @Expose
    private String StoreName;
    @Expose
    private double Latitude;
    @Expose
    private double Longitude;
    @Expose
    private String FoodCategory;
    @Expose
    private double Stars;
    @Expose
    private int NoOfVotes;
    @Expose
    private String StoreLogo;
    @Expose
    private List<Product> Products;

    public Store(String StoreName, double Latitude, double Longitude, String FoodCategory, double Stars, int NoOfVotes, String StoreLogo, List<Product> Products) {
        this.StoreName = StoreName;
        this.Latitude = Latitude;
        this.Longitude = Longitude;
        this.FoodCategory = FoodCategory;
        this.Stars = Stars;
        this.NoOfVotes = NoOfVotes;
        this.StoreLogo = StoreLogo;
        this.Products = Products;
    }

    public String getStoreName() {
        return StoreName;
    }

    public void setStoreName(String StoreName) {
        this.StoreName = StoreName;
    }

    public double getLatitude() {
        return Latitude;
    }

    public void setLatitude(double Latitude) {
        this.Latitude = Latitude;
    }

    public double getLongitude() {
        return Longitude;
    }

    public void setLongitude(double Longitude) {
        this.Longitude = Longitude;
    }

    public String getFoodCategory() {
        return FoodCategory;
    }

    public void setFoodCategory(String FoodCategory) {
        this.FoodCategory = FoodCategory;
    }

    public double getStars() {
        return Stars;
    }

    public void setStars(double Stars) {
        this.Stars = Stars;
    }

    public int getNoOfVotes() {
        return NoOfVotes;
    }

    public void setNoOfVotes(int NoOfVotes) {
        this.NoOfVotes = NoOfVotes;
    }

    public List<Product> getProducts() {
        return Products;
    }

    public void setProducts(List<Product> Products) {
        this.Products = Products;
    }

    public String getStoreLogo() {
        return StoreLogo;
    }

    public void setStoreLogo(String StoreLogo) {
        this.StoreLogo = StoreLogo;
    }

    public String getStorePriceRange(){

        if (this.Products.isEmpty()){
            return "$";
        } else {

            double count = 0;

            for (Product p : this.Products){
                count+=p.getPrice();
            }

            double average = count / this.Products.size();

            if (average <= 5){
                return "$";
            } else if (average <= 15){
                return "$$";
            } else {
                return "$$$";
            }

        }

    }

    public void addStarRating (int stars){

        if (stars <= 0){
            stars = 1;
        } else if (stars >= 5){
            stars = 5;
        }

        this.Stars = (this.Stars * this.NoOfVotes + stars) / (this.NoOfVotes + 1);
        this.NoOfVotes++;

    }

    @Override
    public String toString() {
        return "StoreName = '" + StoreName + '\'' +
                ", Latitude = " + Latitude +
                ", Longitude = " + Longitude +
                ", FoodCategory = '" + FoodCategory + '\'' +
                ", Stars = " + Stars +
                ", NoOfVotes = " + NoOfVotes +
                ", Products = " + Products + " "+
                '}';
    }

}