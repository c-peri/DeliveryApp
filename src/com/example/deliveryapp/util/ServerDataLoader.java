package com.example.deliveryapp.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import java.io.*;
import java.awt.image.BufferedImage;
import java.util.List;
import javax.imageio.ImageIO;

public class ServerDataLoader {

    private static final String SERVER_IMAGES_BASE_DIRECTORY = ".";

    /**
     * Iterates through a given list of Store objects and populates their StoreLogoBytes field
     * by reading the image data from the path specified in StoreLogoPath.
     * This method is intended to be called on the server just before sending the List<Store> to the client.
     *
     * @param stores The List of Store objects that needs to have their logo bytes populated.
     * This list should already be filtered or prepared.
     * @return The same List of Store objects, now with their StoreLogoBytes fields populated.
     */
    public static List<Store> populateStoreLogosForClient(List<Store> stores) {

        if (stores == null || stores.isEmpty()) {
            System.out.println("Server: No stores provided to populate logos.");
            return stores;
        }

        for (Store store : stores) {

            String relativeLogoPath = store.getStoreLogo();

            if (relativeLogoPath != null && !relativeLogoPath.isEmpty()) {

                String fullImagePath = SERVER_IMAGES_BASE_DIRECTORY + File.separator + relativeLogoPath;
                File imageFile = new File(fullImagePath);

                if (imageFile.exists()) {

                    try {
                        BufferedImage bImage = ImageIO.read(imageFile);

                        if (bImage != null) {

                            ByteArrayOutputStream bos = new ByteArrayOutputStream();

                            ImageIO.write(bImage, "png", bos);
                            byte[] imageBytes = bos.toByteArray();
                            bos.close();
                            store.setStoreLogoBytes(imageBytes);

                        } else {

                            System.err.println("Server: Could not decode image from file: " + fullImagePath + " (possibly corrupted or unsupported format)");
                            store.setStoreLogoBytes(null);

                        }

                    } catch (IOException e) {

                        System.err.println("Server: Error loading or encoding image " + fullImagePath + " for store " + store.getStoreName() + ": " + e.getMessage());
                        e.printStackTrace();
                        store.setStoreLogoBytes(null);

                    }

                } else {

                    System.err.println("Server: Image file not found at expected path: " + fullImagePath + " for store: " + store.getStoreName());
                    store.setStoreLogoBytes(null);

                }

            } else {

                System.out.println("Server: No logo path specified for store: " + store.getStoreName() + ". StoreLogoBytes will be null.");
                store.setStoreLogoBytes(null);

            }
        }

        return stores;

    }

}
