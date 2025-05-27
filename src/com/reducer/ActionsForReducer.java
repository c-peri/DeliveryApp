package com.reducer;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.util.ActionWrapper;

import java.util.List;

import java.io.ObjectOutputStream;
import java.net.Socket;

public class ActionsForReducer implements Runnable {

    private final String masterHost;
    private final int masterPort;
    private final Object workerResults;
    private String jobID;

    public ActionsForReducer(String masterHost, int masterPort, Object workerResults, String jobID) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.workerResults = workerResults;
        this.jobID = jobID;
    }

    @Override
    public void run() {

        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            String action;

            if (workerResults instanceof List<?>) {
                action = "mapped_store_results";
            } else if (workerResults instanceof String) {
                action = "confirmation_from_worker";
            } else {
                action = "unknown_result";
            }

            ActionWrapper wrapper = new ActionWrapper(workerResults, action, jobID);
            out.writeObject(wrapper);
            out.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}