package com.example.deliveryapp.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class JobCoordinator {

    public enum JobStatus {
        WAITING,
        SEARCH_DONE,
        COMPLETED
    }

    private static final Map<UUID, Object> jobLocks = new HashMap<>();
    private static final Map<UUID, JobStatus> jobStatuses = new HashMap<>();

    public static synchronized Object getLock(UUID jobId) {
        return jobLocks.computeIfAbsent(jobId, k -> new Object());
    }

    public static synchronized void setStatus(UUID jobId, JobStatus status) {
        jobStatuses.put(jobId, status);
    }

    public static synchronized JobStatus getStatus(UUID jobId) {
        return jobStatuses.getOrDefault(jobId, JobStatus.WAITING);
    }

    public static synchronized void remove(UUID jobId) {
        jobLocks.remove(jobId);
        jobStatuses.remove(jobId);
    }
}