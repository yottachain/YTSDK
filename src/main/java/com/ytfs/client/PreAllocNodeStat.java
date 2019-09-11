package com.ytfs.client;

import com.ytfs.common.net.P2PUtils;
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

    public long getDelayTimes() {
        long oktimes = okDelayTimes.get();
        long count = okTimes.get();
        long errcount = errTimes.get();
        long times = count == 0 ? 0 : (oktimes / count);
        if (times == 0 && errcount != 0) {
            return errcount * 1000;
        }
        return times * errcount + times;
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

    public void disconnet() {
        P2PUtils.remove(this.getNodeid());
    }

}
