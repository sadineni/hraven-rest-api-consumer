package org.apache.hraven.restconsumer.service.dto;

import com.google.gson.annotations.Expose;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.Set;

@Entity(name = "HRavenJobInfo")
public class HRavenJobInfo implements Comparable<HRavenJobInfo> {
    @Id
    @Expose
    private String jobId;
    @Expose
    private String status;
    @Expose
    private Timestamp submitTime;
    @Expose
    private Timestamp launchTime;
    @Expose
    private Timestamp finishTime;
    @Expose
    private long hdfsBytesRead;
    @Expose
    private long hdfsBytesWritten;
    @Expose
    private double cost;
    @Expose
    private long runTime;
    @Expose
    private boolean firstJobInFlow;
    @Expose
    private boolean lastJobInFlow;
    @Expose
    private String clusterName;

    @OneToMany(mappedBy = "hRavenJobInfo", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Expose
    private Set<HRavenTaskInfo> tasks;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn
    private HRavenFlowInfo hRavenFlowInfo;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Timestamp getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Timestamp submitTime) {
        this.submitTime = submitTime;
    }

    public Timestamp getLaunchTime() {
        return launchTime;
    }

    public void setLaunchTime(Timestamp launchTime) {
        this.launchTime = launchTime;
    }

    public Timestamp getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Timestamp finishTime) {
        this.finishTime = finishTime;
    }

    public long getHdfsBytesRead() {
        return hdfsBytesRead;
    }

    public void setHdfsBytesRead(long hdfsBytesRead) {
        this.hdfsBytesRead = hdfsBytesRead;
    }

    public long getHdfsBytesWritten() {
        return hdfsBytesWritten;
    }

    public void setHdfsBytesWritten(long hdfsBytesWritten) {
        this.hdfsBytesWritten = hdfsBytesWritten;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public long getRunTime() {
        return runTime;
    }

    public void setRunTime(long runTime) {
        this.runTime = runTime;
    }

    public Set<HRavenTaskInfo> getTasks() {
        return tasks;
    }

    public void setTasks(Set<HRavenTaskInfo> tasks) {
        this.tasks = tasks;
    }

    public HRavenFlowInfo gethRavenFlowInfo() {
        return hRavenFlowInfo;
    }

    public void sethRavenFlowInfo(HRavenFlowInfo hRavenFlowInfo) {
        this.hRavenFlowInfo = hRavenFlowInfo;
    }

    public boolean isFirstJobInFlow() {
        return firstJobInFlow;
    }

    public void setFirstJobInFlow(boolean firstJobInFlow) {
        this.firstJobInFlow = firstJobInFlow;
    }

    public boolean isLastJobInFlow() {
        return lastJobInFlow;
    }

    public void setLastJobInFlow(boolean lastJobInFlow) {
        this.lastJobInFlow = lastJobInFlow;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public int compareTo(HRavenJobInfo o) {
        return this.getLaunchTime().compareTo(getLaunchTime());
    }
}
