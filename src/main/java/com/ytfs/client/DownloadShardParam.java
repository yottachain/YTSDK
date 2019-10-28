package com.ytfs.client;

import io.yottachain.nodemgmt.core.vo.Node;

public class DownloadShardParam {
    
    public static int Max_Retry_Times=10;

    private Node node;
    private byte[] VHF;
    private int retryTime = 0;

    /**
     * @return the node
     */
    public Node getNode() {
        return node;
    }

    /**
     * @param node the node to set
     */
    public void setNode(Node node) {
        this.node = node;
    }

    /**
     * @return the VHF
     */
    public byte[] getVHF() {
        return VHF;
    }

    /**
     * @param VHF the VHF to set
     */
    public void setVHF(byte[] VHF) {
        this.VHF = VHF;
    }

    /**
     * @return the retryTime
     */
    public int getRetryTime() {
        return retryTime;
    }

    /**
     * @param retryTime the retryTime to set
     */
    public void setRetryTime(int retryTime) {
        this.retryTime = retryTime;
    }

    public void addRetryTime() {
        this.retryTime++;
    }

}
