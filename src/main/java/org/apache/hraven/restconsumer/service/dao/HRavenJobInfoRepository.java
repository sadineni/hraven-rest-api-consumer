package org.apache.hraven.restconsumer.service.dao;

import org.apache.hraven.restconsumer.service.dto.HRavenJobInfo;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HRavenJobInfoRepository extends JpaRepository<HRavenJobInfo, String> {
}
