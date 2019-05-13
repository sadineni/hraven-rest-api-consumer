package org.apache.hraven.restconsumer.service.dao;

import org.apache.hraven.restconsumer.service.dto.HRavenFlowInfo;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HRavenFlowInfoRepository extends JpaRepository<HRavenFlowInfo, String> {

}
