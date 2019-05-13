package org.apache.hraven.restconsumer.service.dao;

import org.apache.hraven.restconsumer.service.dto.HRavenTaskInfo;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HRavenTaskInfoRepository extends JpaRepository<HRavenTaskInfo, String>  {
}
