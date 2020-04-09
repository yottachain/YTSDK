package com.ytfs.client;

import com.ytfs.service.packet.user.PreAllocNode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PreAllocNodeStat extends PreAllocNode {

    private final AtomicLong okDelayTimes = new AtomicLong(0);//总耗时
    private final AtomicInteger okTimes = new AtomicInteger(0);//成功次数
    private final AtomicInteger errTimes = new AtomicInteger(0);//错误次数
    private final AtomicInteger busyTimes = new AtomicInteger(0);//错误次数
    private long resetTime = System.currentTimeMillis();
    private int snid;

    public PreAllocNodeStat() {
    }

    public PreAllocNodeStat(PreAllocNode node,int snid) {
        init(node,snid);
    }

    public void setERR() {
        errTimes.incrementAndGet();
    }

    public void setBusy() {
        busyTimes.incrementAndGet();
    }

    public void setOK(long time) {
        okDelayTimes.addAndGet(time);
        okTimes.incrementAndGet();
    }

    static int busytimes = 15000;
    static int errtimes = 25000;

    static {
        String s = System.getenv("P2PHOST_GRPCCLI_TIMEOUT");
        s = s == null || s.trim().isEmpty() ? "10000" : s.trim();
        try {
            busytimes = Integer.parseInt(s);
            errtimes = busytimes + 10000;
        } catch (Exception d) {
        }
    }

    public long getDelayTimes() {
        long oktimes = okDelayTimes.get();
        long count = okTimes.get();
        long errcount = errTimes.get();
        long busycount = busyTimes.get();
        if (count == 0) {
            if (errcount == 0 && busycount == 0) {
                return 0;
            } else {
                return (busytimes * busycount + errtimes * errcount) / (busycount + errcount);
            }
        } else {
            long times = oktimes / count;
            if (times > errtimes) {
                return times;
            } else {
                return (oktimes + errtimes * errcount + busytimes * busycount) / (count + errcount + busycount);
            }
        }
    }

    public final void init(PreAllocNode node,int snid) {
        this.setId(node.getId());
        this.setNodeid(node.getNodeid());
        this.setPubkey(node.getPubkey());
        this.setAddrs(node.getAddrs());
        this.setTimestamp(node.getTimestamp());
        this.setSign(node.getSign());
        this.snid=snid;
    }

    /**
     * @return the snid
     */
    public int getSnid() {
        return snid;
    }

    /**
     * @param snid the snid to set
     */
    public void setSnid(int snid) {
        this.snid = snid;
    }

    public void resetStat() {
        if (System.currentTimeMillis() - resetTime > 1000 * 60 * 10) {
            okDelayTimes.set(0);
            errTimes.set(0);
            busyTimes.set(0);
            okTimes.set(0);
            resetTime = System.currentTimeMillis();
        }
    }
}
