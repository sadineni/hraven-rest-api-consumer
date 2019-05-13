package org.apache.hraven.restconsumer.service.dto;

import com.google.gson.annotations.Expose;

import javax.persistence.*;
import java.util.List;

@Entity(name = "HRavenFlowInfo")
public class HRavenFlowInfo {

    @Id
    @Expose
    private String flowNameWithRunId;
    @Expose
    private String flowName;
    @Expose
    private String runId;
    @Expose
    private String clusterName;

    @OneToMany(mappedBy = "hRavenFlowInfo", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Expose
    private List<HRavenJobInfo> jobInfoList;

    public String getFlowNameWithRunId() {
        return flowNameWithRunId;
    }

    public void setFlowNameWithRunId(String flowNameWithRunId) {
        this.flowNameWithRunId = flowNameWithRunId;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public List<HRavenJobInfo> getJobInfoList() {
        return jobInfoList;
    }

    public void setJobInfoList(List<HRavenJobInfo> jobInfoList) {
        this.jobInfoList = jobInfoList;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String toString() {
        return "HRavenFlowInfo{" +
                "flowNameWithRunId='" + flowNameWithRunId + '\'' +
                ", flowName='" + flowName + '\'' +
                ", runId='" + runId + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", jobInfoList=" + jobInfoList +
                '}';
    }
}
