package com.ytfs.client;

import java.util.List;

public class Configurator {

    private int superNodeNum;   //超级节点编号 (0-31)
    private String superNodeID;  //超级节点id !=null trim
    private List<String> superNodeAddrs;  //超级节点地址
    private int port;        //本地端口 !=0
    private int userID;      //用户ID (1---max)
    private String KUEp;     //用户公钥
    private String KUSp;     //用户私钥
    private String tmpFilePath;

    /**
     * @return the superNodeNum
     */
    public int getSuperNodeNum() {
        return superNodeNum;
    }

    /**
     * @param superNodeNum the superNodeNum to set
     */
    public void setSuperNodeNum(int superNodeNum) {
        this.superNodeNum = superNodeNum;
    }

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
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return the userID
     */
    public int getUserID() {
        return userID;
    }

    /**
     * @param userID the userID to set
     */
    public void setUserID(int userID) {
        this.userID = userID;
    }

    /**
     * @return the KUEp
     */
    public String getKUEp() {
        return KUEp;
    }

    /**
     * @param KUEp the KUEp to set
     */
    public void setKUEp(String KUEp) {
        this.KUEp = KUEp;
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

}
