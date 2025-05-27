package com.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StoreReducer {

    public List<Store> reduce(String key, List<Store> stores) {

        if (key == null || stores == null || stores.isEmpty()) {
            return Collections.emptyList();
        }

        switch (key) {
            case "within_range", "filtered_store":
                return new ArrayList<>(stores);
            default:
                return Collections.emptyList();
        }

    }

}
