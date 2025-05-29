package com.example.deliveryapp.util;

/*
 * @author Alexandra-Maria Mazi || p3220111@aueb.gr
 * @author Christina Perifana   || p3220160@aueb.gr
 */

import java.io.Serial;
import java.io.Serializable;

public class ActionWrapper implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Object obj;
    private String action;
    private String jobID;

    public ActionWrapper(Object obj, String action, String jobID) {
        this.obj = obj;
        this.action = action;
        this.jobID = jobID;
    }

    public Object getObject() { return obj; }

    public void setObject(String s){
        String obj2 = (String) obj;
        obj2 = obj2+"_"+s;
        this.obj = obj2;
    }

    public String getAction() { return action; }
    public String getJobID(){ return jobID;}

}