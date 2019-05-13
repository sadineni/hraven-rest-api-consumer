package org.apache.hraven.restconsumer.service.dto;

import java.util.Comparator;

public class HRavenJobInfoComparator implements Comparator<HRavenJobInfo> {
    private JobCompareParam jobCompareParam;
    private Order order;

    public HRavenJobInfoComparator(JobCompareParam jobCompareParam, Order order) {
        this.jobCompareParam = jobCompareParam;
        this.order = order;
    }

    @Override
    public int compare(HRavenJobInfo o1, HRavenJobInfo o2) {
        if(jobCompareParam == JobCompareParam.BY_LAUNCH_TIME) {
            if(order == Order.ASC) {
                return o1.getLaunchTime().compareTo(o2.getLaunchTime());
            } else {
                return o2.getLaunchTime().compareTo(o1.getLaunchTime());
            }
        }
        if(jobCompareParam == JobCompareParam.BY_FINISH_TIME) {
            if(order == Order.ASC) {
                return o1.getFinishTime().compareTo(o2.getFinishTime());
            } else {
                return o2.getFinishTime().compareTo(o1.getFinishTime());
            }
        }
        return 0;
    }

    public static enum JobCompareParam  {
        BY_LAUNCH_TIME, BY_FINISH_TIME;
    }

    public static enum Order  {
        ASC, DESC;
    }

}
