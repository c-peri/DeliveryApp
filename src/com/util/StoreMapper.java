package com.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

public class StoreMapper {

    private final double userLat;
    private final double userLon;
    private final FilterMode filterMode;

    private String foodCategory;
    private int minStars;
    private String priceRange;

    public StoreMapper(double userLat, double userLon, FilterMode filterMode) {
        this.userLat = userLat;
        this.userLon = userLon;
        this.filterMode = filterMode;
    }

    public void setFoodCategory(String foodCategory) {
        this.foodCategory = foodCategory;
    }

    public void setMinStars(int minStars) {
        this.minStars = minStars;
    }

    public void setPriceRange(String priceRange) {
        this.priceRange = priceRange;
    }

    public List<AbstractMap.SimpleEntry<String, Store>> map(List<Store> stores) {

        List<AbstractMap.SimpleEntry<String, Store>> result = new ArrayList<>();

        for (Store store : stores) {

            double distance = GeoUtils.haversine(userLat, userLon, store.getLatitude(), store.getLongitude());

            if (distance > 5.0) continue;

            switch (filterMode) {

                case LOCATION:
                    result.add(new AbstractMap.SimpleEntry<>("within_range", store));
                    break;
                case LOCATION_AND_CATEGORY:
                    if (store.getFoodCategory().equalsIgnoreCase(foodCategory)) {result.add(new AbstractMap.SimpleEntry<>("filtered_store", store)); }
                    break;
                case LOCATION_AND_STARS:
                    if (store.getStars() >= minStars) { result.add(new AbstractMap.SimpleEntry<>("filtered_store", store)); }
                    break;
                case LOCATION_AND_PRICE_RANGE:
                    if (store.getStorePriceRange().equals(priceRange)) { result.add(new AbstractMap.SimpleEntry<>("filtered_store", store)); }
                    break;

            }

        }

        return result;

    }

}
