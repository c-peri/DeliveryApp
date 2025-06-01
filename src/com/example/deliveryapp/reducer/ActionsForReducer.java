package com.example.deliveryapp.reducer;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import com.example.deliveryapp.util.ActionWrapper;
import com.example.deliveryapp.util.JobCoordinator;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.UUID;

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

            ActionWrapper wrapper = new ActionWrapper(workerResults, action, jobID);
            out.writeObject(wrapper);
            out.flush();
            Object lock = JobCoordinator.getLock(UUID.fromString(jobID));

            synchronized (lock) {
                while (!JobCoordinator.getStatus(UUID.fromString(jobID)).equals("COMPLETED")) {
                    lock.wait(500);
                }
            }

            System.out.println("Job complete: " + jobID);

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}