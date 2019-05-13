package org.apache.hraven.restconsumer.service.executors;

import com.google.gson.*;
import org.apache.hraven.restconsumer.service.dao.HravenPersistenceDao;
import org.apache.hraven.restconsumer.service.dto.HRavenFlowInfo;
import org.apache.hraven.restconsumer.service.dto.HRavenJobInfo;
import org.apache.hraven.restconsumer.service.dto.HRavenTaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class JobServiceConsumer implements Callable<HRavenJobInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(JobServiceConsumer.class);

    private RestTemplate restTemplate = new RestTemplate();
    private HRavenJobInfo jobFromFlowRun;
    private String clusterName;
    private String restTaskEndPoint;
    private HRavenFlowInfo flowRun;
    private HravenPersistenceDao hravenPersistenceDao;

    public JobServiceConsumer(HRavenFlowInfo flowRun, HRavenJobInfo jobFromFlowRun, String clusterName, String restTaskEndPoint, HravenPersistenceDao hravenPersistenceDao) {
        this.flowRun = flowRun;
        this.jobFromFlowRun = jobFromFlowRun;
        this.clusterName = clusterName;
        this.restTaskEndPoint = restTaskEndPoint;
        this.hravenPersistenceDao = hravenPersistenceDao;
    }

    @Override
    public HRavenJobInfo call() {
        String taskEndPoint = generateTaskEndpoint(clusterName, jobFromFlowRun.getJobId());
        Path path = Paths.get("/Users/akumarsadineni/work/google/hraven_analysis/failed_job_api.txt." + new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").format(new Date()));
        try {
            ResponseEntity<String> taskResponse = restTemplate.getForEntity(taskEndPoint, String.class);
            String taskResponseString = taskResponse.getBody();
            Set<HRavenTaskInfo> tasksList = new LinkedHashSet<>();
            jobFromFlowRun.setTasks(tasksList);

            // get tasks
            Gson gson = new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation().create();
            JsonArray taskArray = gson.fromJson(taskResponseString, JsonArray.class);
            taskArray.forEach(task -> {
                if (!task.getAsJsonObject().get("taskId").getAsString().isEmpty()) {
                    HRavenTaskInfo hRavenTaskInfo = buildTask(task);
                    hRavenTaskInfo.sethRavenJobInfo(jobFromFlowRun);
                    tasksList.add(hRavenTaskInfo);
                }
            });
        } catch (Exception e) {
            try {
                Files.write(path, ("Unable to pull tasks for job - " + jobFromFlowRun.getJobId() + ", flow - "
                                + flowRun.getFlowName()).getBytes(),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            LOG.error("***** job endpoint not reachable [START ]******");
            LOG.error("Unable to pull tasks for job - {}, flow - {}", jobFromFlowRun.getJobId(), flowRun.getFlowName());
            LOG.error("URL {}", taskEndPoint);
            LOG.error("Exception - {}", e.getMessage());
            LOG.error("Exception ", e);
            LOG.error("***** job endpoint not reachable [END] ******");
        }
        if(hravenPersistenceDao != null) {
            hravenPersistenceDao.persistHRavenTaskInfo(jobFromFlowRun.getTasks().toArray(new HRavenTaskInfo[0]));
        }
        return jobFromFlowRun;
    }

    private String generateTaskEndpoint(String clusterName, String jobId) {
        return restTaskEndPoint + "/" + clusterName + "/" + jobId + "?limit=10000&include=taskId&include=startTime&include=finishTime&include=counters";
    }

    private HRavenTaskInfo buildTask(JsonElement task) {
        HRavenTaskInfo hRavenTaskInfo = new HRavenTaskInfo();
        hRavenTaskInfo.setTaskId(task.getAsJsonObject().get("taskId").getAsString());
        hRavenTaskInfo.setStartTime(new Timestamp(task.getAsJsonObject().get("startTime").getAsLong()));
        hRavenTaskInfo.setFinishTime(new Timestamp(task.getAsJsonObject().get("finishTime").getAsLong()));
        JsonObject counters = ((JsonObject) task).getAsJsonObject("counters");
        if (counters != null && counters.size() != 0) {
            hRavenTaskInfo.setHdfsBytesRead(readMetricIfPresent(counters, "HDFS_BYTES_READ"));
            hRavenTaskInfo.setHdfsBytesWritten(readMetricIfPresent(counters, "HDFS_BYTES_WRITTEN"));
            hRavenTaskInfo.setHdfsReadOps(readMetricIfPresent(counters, "HDFS_READ_OPS"));
            hRavenTaskInfo.setHdfsWriteOps(readMetricIfPresent(counters, "HDFS_WRITE_OPS"));
        }
        return hRavenTaskInfo;
    }

    private long readMetricIfPresent(JsonObject counters, String metrics) {
        if (counters.get("org.apache.hadoop.mapreduce.FileSystemCounter") != null) {
            if (counters.get("org.apache.hadoop.mapreduce.FileSystemCounter").getAsJsonObject().get(metrics) != null) {
                return counters.get("org.apache.hadoop.mapreduce.FileSystemCounter").getAsJsonObject().get(metrics).getAsLong();
            } else {
                LOG.warn("Missing metrics {}", metrics);
                return -1;
            }
        } else {
            LOG.warn("Missing org.apache.hadoop.mapreduce.FileSystemCounter");
            return -1;
        }
    }
}
