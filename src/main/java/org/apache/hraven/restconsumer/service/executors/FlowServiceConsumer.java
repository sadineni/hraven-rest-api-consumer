package org.apache.hraven.restconsumer.service.executors;

import com.google.gson.*;
import org.apache.hraven.restconsumer.service.dao.HravenPersistenceDao;
import org.apache.hraven.restconsumer.service.dto.HRavenFlowInfo;
import org.apache.hraven.restconsumer.service.dto.HRavenJobInfo;
import org.apache.hraven.restconsumer.service.dto.HRavenJobInfoComparator;
import org.apache.hraven.restconsumer.service.dto.HRavenTaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class FlowServiceConsumer implements Callable<HRavenFlowInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(FlowServiceConsumer.class);

    private JsonElement flowRun;
    private String clusterName;
    private String restTaskEndPoint;
    private int jobExecutorThreads;
    private HravenPersistenceDao hravenPersistenceDao;
    private ExecutorCompletionService jobServiceExecutorService;
    private boolean pullTasks;

    public FlowServiceConsumer(String clusterName, JsonElement flowRun, String restTaskEndPoint, int jobExecutorThreads, HravenPersistenceDao hravenPersistenceDao, boolean pullTasks) {
        this.flowRun = flowRun;
        this.clusterName = clusterName;
        this.restTaskEndPoint = restTaskEndPoint;
        this.jobExecutorThreads = jobExecutorThreads;
        this.hravenPersistenceDao = hravenPersistenceDao;
        if(jobExecutorThreads > 1) {
            this.jobServiceExecutorService
                    = new ExecutorCompletionService(new ThreadPoolExecutor(jobExecutorThreads, jobExecutorThreads, 24,
                    TimeUnit.HOURS, new LinkedBlockingQueue<>(10)));
        } else {
            this.jobServiceExecutorService
                    = new ExecutorCompletionService(new ThreadPoolExecutor(1, 1, 24,
                    TimeUnit.HOURS, new LinkedBlockingQueue<>(10)));
        }
        this.pullTasks = pullTasks;
    }

    @Override
    public HRavenFlowInfo call() throws Exception {
        HRavenFlowInfo hRavenFlowInfo = buildFlow(flowRun);
        if (hravenPersistenceDao != null) {
            hravenPersistenceDao.persistHRavenFlowInfo(hRavenFlowInfo);
        }

        JsonArray jobs = flowRun.getAsJsonObject().get("jobs").getAsJsonArray();
        hRavenFlowInfo.setJobInfoList(buildJobs(jobs));

        if (hravenPersistenceDao != null) {
            hravenPersistenceDao.persistHRavenJobInfo(hRavenFlowInfo.getJobInfoList().toArray(new HRavenJobInfo[0]));
        }

        Gson gson = new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation().create();

        if(pullTasks) {
            hRavenFlowInfo.getJobInfoList().forEach(jobObject -> {
                this.jobServiceExecutorService.submit(new JobServiceConsumer(hRavenFlowInfo, jobObject, clusterName, restTaskEndPoint, hravenPersistenceDao));
            });
            if (hravenPersistenceDao != null) {
                hRavenFlowInfo.getJobInfoList().forEach(job -> {
                    hravenPersistenceDao.persistHRavenTaskInfo(job.getTasks().toArray(new HRavenTaskInfo[0]));
                });
            }
        }
        //LOG.info("Result for flow - {}", gson.toJson(hRavenFlowInfo));
        return hRavenFlowInfo;
    }

    private List<HRavenJobInfo> buildJobs(JsonArray jobs) {
        List<HRavenJobInfo> jobList = new LinkedList<>();
        jobs.forEach(job -> {
            JsonObject jobObject = job.getAsJsonObject();
            HRavenJobInfo jobInfo = new HRavenJobInfo();
            jobInfo.setJobId(jobObject.get("jobId").getAsString());
            jobInfo.setStatus(jobObject.get("status").getAsString());
            jobInfo.setHdfsBytesRead(jobObject.get("hdfsBytesRead").getAsLong());
            jobInfo.setHdfsBytesWritten(jobObject.get("hdfsBytesWritten").getAsLong());
            jobInfo.setCost(jobObject.get("cost").getAsDouble());
            jobInfo.setRunTime(jobObject.get("runTime").getAsLong());
            jobInfo.setSubmitTime(new Timestamp(jobObject.get("submitTime").getAsLong()));
            jobInfo.setLaunchTime(new Timestamp(jobObject.get("launchTime").getAsLong()));
            jobInfo.setFinishTime(new Timestamp(jobObject.get("finishTime").getAsLong()));
            jobInfo.setClusterName(clusterName);
            jobList.add(jobInfo);
        });
        Collections.sort(jobList, new HRavenJobInfoComparator(HRavenJobInfoComparator.JobCompareParam.BY_LAUNCH_TIME, HRavenJobInfoComparator.Order.ASC));
        jobList.get(0).setFirstJobInFlow(true);
        Collections.sort(jobList, new HRavenJobInfoComparator(HRavenJobInfoComparator.JobCompareParam.BY_FINISH_TIME, HRavenJobInfoComparator.Order.DESC));
        jobList.get(0).setLastJobInFlow(true);
        //LOG.info("Jobs after marking {}", jobList);
        return jobList;
    }

    private HRavenFlowInfo buildFlow(JsonElement flow) {
        HRavenFlowInfo hRavenFlowInfo = new HRavenFlowInfo();
        hRavenFlowInfo.setFlowNameWithRunId(flow.getAsJsonObject().get("flowName").getAsString() + "_" + flow.getAsJsonObject().get("runId").getAsString());
        hRavenFlowInfo.setFlowName(flow.getAsJsonObject().get("flowName").getAsString());
        hRavenFlowInfo.setRunId(flow.getAsJsonObject().get("runId").getAsString());
        hRavenFlowInfo.setClusterName(clusterName);
        return hRavenFlowInfo;
    }
}
