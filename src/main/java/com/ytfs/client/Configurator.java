package com.ytfs.client;

import java.util.List;

public class Configurator {

    private String superNodeID;  //超级节点id !=null trim
    private List<String> superNodeAddrs;  //超级节点地址
    private String username;
    private String KUSp;     //用户私钥
    private String tmpFilePath;

    /**
     * @return the superNodeID
     */
    public String getSuperNodeID() {
        return superNodeID;
    }

    /**
     * @param superNodeID the superNodeID to set
     */
    public void setSuperNodeID(String superNodeID) {
        this.superNodeID = superNodeID;
    }

    /**
     * @return the KUSp
     */
    public String getKUSp() {
        return KUSp;
    }

    /**
     * @param KUSp the KUSp to set
     */
    public void setKUSp(String KUSp) {
        this.KUSp = KUSp;
    }

    /**
     * @return the superNodeAddrs
     */
    public List<String> getSuperNodeAddrs() {
        return superNodeAddrs;
    }

    /**
     * @param superNodeAddrs the superNodeAddrs to set
     */
    public void setSuperNodeAddrs(List<String> superNodeAddrs) {
        this.superNodeAddrs = superNodeAddrs;
    }

    /**
     * @return the tmpFilePath
     */
    public String getTmpFilePath() {
        return tmpFilePath;
    }

    /**
     * @param tmpFilePath the tmpFilePath to set
     */
    public void setTmpFilePath(String tmpFilePath) {
        this.tmpFilePath = tmpFilePath;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }
}
