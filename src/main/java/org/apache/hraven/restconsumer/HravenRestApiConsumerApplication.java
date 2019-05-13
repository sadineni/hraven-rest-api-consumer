package org.apache.hraven.restconsumer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.hraven.restconsumer.service.RestConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Main application launcher
 *
 * @author Anil Sadineni
 */
@SpringBootApplication
public class HravenRestApiConsumerApplication implements ApplicationRunner {

	private static final Logger LOG = LoggerFactory.getLogger(HravenRestApiConsumerApplication.class);
	private static final String[] mandatoryArgs = new String[] {"startTime", "endTime", "clusterName"};

	@Autowired
	private RestConsumerService restConsumerService;

	@Value("${haraven.export.csv.path}")
	private String hravenExportPath;

	public static void main(String[] args) {
		SpringApplication.run(HravenRestApiConsumerApplication.class, args);
		System.exit(0);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		checkMandatoryArgs(args);
		long startTimeMillis = Long.parseLong(args.getOptionValues("startTime").get(0));
		long endTimeMillis = Long.parseLong(args.getOptionValues("endTime").get(0));
		String clusterName = args.getOptionValues("clusterName").get(0);
		List<FlowIdAndUserInfo> flowIdAndUserInfos = readUserAndFlowIdInfo();
		flowIdAndUserInfos.forEach(flowIdAndUserInfo -> {
			try {
				restConsumerService.consumeService(clusterName,  flowIdAndUserInfo.getUserName(), flowIdAndUserInfo.getFlowId(), startTimeMillis, endTimeMillis, 1000);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	private void checkMandatoryArgs(ApplicationArguments args) {
		Arrays.stream(mandatoryArgs).forEach(arg -> {
			if(!args.containsOption(arg)) {
				throw new IllegalArgumentException("Mandatory argument missing - "+arg);
			}
		} );
	}

	private List<FlowIdAndUserInfo> readUserAndFlowIdInfo() throws IOException {
		List<FlowIdAndUserInfo> flowIdAndUserInfos = new ArrayList<>();
		Reader reader = Files.newBufferedReader(Paths.get(hravenExportPath));
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader("","username","pool","app_id","compute cost","runs","last run (UTC)"));
		csvParser.forEach( record -> {
			FlowIdAndUserInfo flowIdAndUserInfo = new FlowIdAndUserInfo();
			flowIdAndUserInfo.setUserName(record.get(1));
			flowIdAndUserInfo.setFlowId(record.get(3));
			flowIdAndUserInfos.add(flowIdAndUserInfo);
		});
		flowIdAndUserInfos.remove(0);
		LOG.info("Data from file {}", flowIdAndUserInfos);
		return flowIdAndUserInfos;
	}
}
