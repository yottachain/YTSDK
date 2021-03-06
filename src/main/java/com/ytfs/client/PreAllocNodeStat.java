package com.ytfs.client;

import com.ytfs.service.packet.user.PreAllocNode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PreAllocNodeStat extends PreAllocNode {

    private final AtomicLong okDelayTimes = new AtomicLong(0);//总耗时
    private final AtomicInteger okTimes = new AtomicInteger(0);//成功次数
    private final AtomicInteger errTimes = new AtomicInteger(0);//错误次数
    private long resetTime = System.currentTimeMillis();

    public PreAllocNodeStat() {
    }

    public PreAllocNodeStat(PreAllocNode node) {
        init(node);
    }

    public void setERR() {
        errTimes.incrementAndGet();
    }

    public void setOK(long time) {
        okDelayTimes.addAndGet(time);
        okTimes.incrementAndGet();
    }

    public long getDelayTimes() {
        long oktimes = okDelayTimes.get();
        long count = okTimes.get();
        long errcount = errTimes.get();
        if (count == 0) {
            if (errcount == 0) {
                return 0;
            } else {
                return 60000;
            }
        } else {
            long times = oktimes / count;
            if (errcount == 0) {
                return times;
            } else {
                if (times > 60000) {
                    return times;
                } else {
                    return (oktimes + 60000 * errcount) / (oktimes + errcount);
                }
            }
        }
    }

    public final void init(PreAllocNode node) {
        this.setId(node.getId());
        this.setNodeid(node.getNodeid());
        this.setPubkey(node.getPubkey());
        this.setAddrs(node.getAddrs());
        this.setTimestamp(node.getTimestamp());
        this.setSign(node.getSign());
    }

    public void resetStat() {
        if (System.currentTimeMillis() - resetTime > 1000 * 60 * 10) {
            okDelayTimes.set(0);
            errTimes.set(0);
            okTimes.set(0);
            resetTime = System.currentTimeMillis();
        }
    }
}
