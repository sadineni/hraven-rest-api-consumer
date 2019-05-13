package org.apache.hraven.restconsumer.service.dao;

import org.apache.hraven.restconsumer.service.dto.HRavenFlowInfo;
import org.apache.hraven.restconsumer.service.dto.HRavenJobInfo;
import org.apache.hraven.restconsumer.service.dto.HRavenTaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class HravenPersistenceDao {

    private static final Logger LOG = LoggerFactory.getLogger(HravenPersistenceDao.class);
    @Autowired
    private HRavenFlowInfoRepository hRavenFlowInfoRepository;
    @Autowired
    private HRavenJobInfoRepository hRavenJobInfoRepository;
    @Autowired
    private HRavenTaskInfoRepository hRavenTaskInfoRepository;

    public void persistHRavenInfo(HRavenFlowInfo... hRavenFlowInfos) {
        LOG.info("Persisting hRavenFlowInfos "+hRavenFlowInfos.length);
        Arrays.stream(hRavenFlowInfos).forEach(hRavenFlowInfo -> {
            hRavenFlowInfoRepository.save(hRavenFlowInfo);
            if(hRavenFlowInfo.getJobInfoList() != null) {
                hRavenFlowInfo.getJobInfoList().forEach(hRavenJobInfo -> {
                    hRavenJobInfoRepository.save(hRavenJobInfo);
                    if(hRavenJobInfo.getTasks() != null) {
                        hRavenJobInfo.getTasks().forEach(hRavenTaskInfo -> {
                            hRavenTaskInfoRepository.save(hRavenTaskInfo);
                        });
                    }
                });
            }
        });
    }

    public void persistHRavenFlowInfo(HRavenFlowInfo... hRavenFlowInfos) {
        LOG.debug("Persisting hRavenFlowInfos "+hRavenFlowInfos.length);
        Arrays.stream(hRavenFlowInfos).forEach(hRavenFlowInfo -> {
            hRavenFlowInfoRepository.save(hRavenFlowInfo);
        });
    }

    public void persistHRavenJobInfo(HRavenJobInfo... hRavenJobInfos) {
        LOG.debug("Persisting hRavenJobInfos "+hRavenJobInfos.length);
        Arrays.stream(hRavenJobInfos).forEach(hRavenJobInfo -> {
            hRavenJobInfoRepository.save(hRavenJobInfo);
        });
    }

    public void persistHRavenTaskInfo(HRavenTaskInfo... hRavenTaskInfos) {
        LOG.debug("Persisting hRavenTaskInfos "+hRavenTaskInfos.length);
        Arrays.stream(hRavenTaskInfos).forEach(hRavenTaskInfo -> {
            hRavenTaskInfoRepository.save(hRavenTaskInfo);
        });
    }

}
