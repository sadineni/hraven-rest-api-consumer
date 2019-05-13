package org.apache.hraven.restconsumer.service;

import java.io.IOException;

/**
 * Interface to consumer rest end points.
 */
public interface RestConsumerService {

    /**
     * REST endpoint consumer.
     *
     * @param clusterName  - cluster name
     * @param userName - user name
     * @param flowName - flow name
     * @param fromTime - From Time
     * @param toTime - To Time
     * @param limit - limit on results
     * @throws IOException
     */
    void consumeService(String clusterName, String userName, String flowName, long fromTime, long toTime, long limit) throws IOException;
}
