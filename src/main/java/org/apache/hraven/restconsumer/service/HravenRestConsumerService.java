package org.apache.hraven.restconsumer.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hraven.restconsumer.service.dao.HravenPersistenceDao;
import org.apache.hraven.restconsumer.service.executors.FlowServiceConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

import static java.lang.Thread.sleep;

@Component
public class HravenRestConsumerService implements RestConsumerService, InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(HravenRestConsumerService.class);

    @Value("${hraven.flow.endpoint}")
    private String restFlowEndPoint;

    @Value("${hraven.task.endpoint}")
    private String restTaskEndPoint;

    @Value("${hraven.flow.executor.threads}")
    private int flowExecutorThreads;

    @Value("${hraven.job.executor.threads}")
    private int jobExecutorThreads;

    @Value("${persist.results}")
    private boolean persistResults;

    @Value("${hraven.pull.tasks}")
    private boolean pullTasks;

    @Autowired
    private HravenPersistenceDao hravenPersistenceDao;

    private RestTemplate restTemplate = new RestTemplate();

    private ExecutorCompletionService flowExecutorService;

    @Override
    public void consumeService(String clusterName, String userName, String flowName, long fromTime, long toTime, long limit) throws IOException {
        Path path = Paths.get("/Users/akumarsadineni/work/google/hraven_analysis/failed_flow_api_" + new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").format(new Date())+".txt");
        String flowEndPoint = generateFlowEndpoint(clusterName, userName, flowName, fromTime, toTime, limit);
        LOG.info("Flow Endpoint {}", flowEndPoint);
        try {
            ResponseEntity<String> responseEntity = restTemplate.getForEntity(flowEndPoint, String.class);
            String response = responseEntity.getBody();
            Gson gson = new GsonBuilder().setPrettyPrinting().excludeFieldsWithoutExposeAnnotation().create();
            JsonObject flowObject = gson.fromJson(response, JsonObject.class);
            JsonArray flowArray = flowObject.get("values").getAsJsonArray();
            flowArray.forEach(flowRun -> {
                flowExecutorService.submit(new FlowServiceConsumer(clusterName, flowRun, restTaskEndPoint, jobExecutorThreads, persistResults ? hravenPersistenceDao : null, pullTasks));
            });
            //LOG.info("Flows {}", gson.toJson(flows));
        } catch (RuntimeException e) {
            LOG.error("***** flow endpoint not reachable [START ]******");
            LOG.error("Unable to pull flow info - {}", flowName);
            LOG.error("Flow endpoint {} ", flowEndPoint);
            try {
                Files.write(path, ("Unable to pull flow end point - " + flowEndPoint).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            LOG.error("Exception - {}", e.getMessage());
            LOG.error("Exception ", e);
            LOG.error("***** flow endpoint not reachable [END] ******");
        }
    }

    private void relax(long millis) {
        try {
            LOG.info("Sleeping {} millis", millis);
            sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String generateFlowEndpoint(String clusterName, String userName, String flowName, long fromTime, long toTime, long limit) {
        return restFlowEndPoint + "/" + clusterName + "/" + userName + "/"
                + replaceBackSlashInFlowName(flowName) + "?limit=" + limit
                + "&startTime=" + fromTime + "&endTime=" + toTime + "&includeJobs=true";
    }

    private String replaceBackSlashInFlowName(String flowName) {
        if (StringUtils.indexOf(flowName, "/") != -1) {
            String replacedFlowName = StringUtils.replace(flowName, "/", "%2F");
            LOG.warn("Flowname has / and replaced string {}", replacedFlowName);
            return replacedFlowName;
        }
        return flowName;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (flowExecutorThreads > 1) {
            flowExecutorService = new ExecutorCompletionService(new ThreadPoolExecutor(flowExecutorThreads, flowExecutorThreads, 24, TimeUnit.HOURS, new LinkedBlockingQueue<>(1000)));
        } else {
            flowExecutorService = new ExecutorCompletionService(new ThreadPoolExecutor(1, 1, 24, TimeUnit.HOURS, new LinkedBlockingQueue<>(1000)));
        }
    }
}
