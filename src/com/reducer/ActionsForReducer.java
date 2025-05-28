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
    private String action;
    private String jobID;

    public ActionsForReducer(String masterHost, int masterPort, String action, Object workerResults, String jobID) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.action = action;
        this.workerResults = workerResults;
        this.jobID = jobID;
    }

    @Override
    public void run() {

        try (Socket socket = new Socket(masterHost, masterPort);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            if (workerResults instanceof List<?>) {
                ActionWrapper wrapper = new ActionWrapper(workerResults, this.action, jobID);
                out.writeObject(wrapper);
                out.flush();
            } else if (workerResults instanceof String) {
                ActionWrapper wrapper = new ActionWrapper(workerResults, "confirmation_from_worker", jobID);
                out.writeObject(wrapper);
                out.flush();
            } else {
                ActionWrapper wrapper = new ActionWrapper(workerResults, "unknown_result", jobID);
                out.writeObject(wrapper);
                out.flush();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}