package com.ytfs.client;

import java.util.Comparator;

public class PreAllocNodeComparator implements Comparator<PreAllocNodeStat> {

    @Override
    public int compare(PreAllocNodeStat o1, PreAllocNodeStat o2) {
        if (o1.getDelayTimes() > o2.getDelayTimes()) {
            return 1;
        } else if (o1.getDelayTimes() == o2.getDelayTimes()) {
            return 0;
        } else {
            return -1;
        }
    }

}
