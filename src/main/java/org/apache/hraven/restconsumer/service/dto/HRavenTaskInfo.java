package org.apache.hraven.restconsumer.service.dto;

import com.google.gson.annotations.Expose;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.Objects;

@Entity(name = "HRavenTaskInfo")
public class HRavenTaskInfo {
    @Id
    @Expose
    private String taskId;
    @Expose
    private Timestamp startTime;
    @Expose
    private Timestamp finishTime;
    @Expose
    private long hdfsBytesRead;
    @Expose
    private long hdfsBytesWritten;
    @Expose
    private long hdfsReadOps;
    @Expose
    private long hdfsWriteOps;
    @Expose
    private String clusterName;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn
    private HRavenJobInfo hRavenJobInfo;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
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

    public long getHdfsReadOps() {
        return hdfsReadOps;
    }

    public void setHdfsReadOps(long hdfsReadOps) {
        this.hdfsReadOps = hdfsReadOps;
    }

    public long getHdfsWriteOps() {
        return hdfsWriteOps;
    }

    public void setHdfsWriteOps(long hdfsWriteOps) {
        this.hdfsWriteOps = hdfsWriteOps;
    }

    public HRavenJobInfo gethRavenJobInfo() {
        return hRavenJobInfo;
    }

    public void sethRavenJobInfo(HRavenJobInfo hRavenJobInfo) {
        this.hRavenJobInfo = hRavenJobInfo;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String toString() {
        return "HRavenTaskInfo{" +
                "taskId='" + taskId + '\'' +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", hdfsBytesRead=" + hdfsBytesRead +
                ", hdfsBytesWritten=" + hdfsBytesWritten +
                ", hdfsReadOps=" + hdfsReadOps +
                ", hdfsWriteOps=" + hdfsWriteOps +
                ", clusterName='" + clusterName + '\'' +
                ", hRavenJobInfo=" + hRavenJobInfo +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HRavenTaskInfo that = (HRavenTaskInfo) o;
        return taskId.equals(that.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId);
    }
}
