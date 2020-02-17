package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import java.util.List;

public class Configurator {

    private String superNodeID;  //超级节点id !=null trim
    private List<String> superNodeAddrs;  //超级节点地址
    private String username;
    private String KUSp;     //用户私钥
    private String tmpFilePath;
    private int uploadShardThreadNum = UserConfig.UPLOADSHARDTHREAD;
    private int uploadBlockThreadNum = UserConfig.UPLOADBLOCKTHREAD;
    private int downloadThread = UserConfig.DOWNLOADSHARDTHREAD;
    private long uploadFileMaxMemory = UserConfig.UPLOADFILEMAXMEMORY;
    private int PNN = UserConfig.PNN;
    private int PTR = UserConfig.PTR;
    private int RETRYTIMES = UserConfig.RETRYTIMES;
    private String zipkinServer;

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

    /**
     * @return the downloadThread
     */
    public int getDownloadThread() {
        return downloadThread;
    }

    public void setDownloadThread(String downloadThread) {
        try {
            int num = Integer.parseInt(downloadThread);
            setDownloadThread(num);
        } catch (Exception r) {
        }
    }

    /**
     * @param downloadThread the downloadThread to set
     */
    public void setDownloadThread(int downloadThread) {
        if (downloadThread < 50) {
            downloadThread = 50;
        }
        if (downloadThread > 500) {
            downloadThread = 500;
        }
        this.downloadThread = downloadThread;
    }

    /**
     * @return the uploadBlockThreadNum
     */
    public long getUploadFileMaxMemory() {
        return uploadFileMaxMemory;
    }

    /**
     * @param uploadBlockThreadNum the uploadBlockThreadNum to set
     */
    public void setUploadBlockThreadNum(String uploadBlockThreadNum) {
        try {
            int num = Integer.parseInt(uploadBlockThreadNum);
            setUploadBlockThreadNum(num);
        } catch (Exception r) {
        }

    }

    /**
     * @param uploadBlockThreadNum the uploadBlockThreadNum to set
     */
    public void setUploadBlockThreadNum(int uploadBlockThreadNum) {
        if (uploadBlockThreadNum < 3) {
            uploadBlockThreadNum = 3;
        }
        if (uploadBlockThreadNum > 500) {
            uploadBlockThreadNum = 500;
        }
        this.uploadBlockThreadNum = uploadBlockThreadNum;
    }

    public void setUploadFileMaxMemory(String uploadFileMaxMemory) {
        try {
            long num = Long.parseLong(uploadFileMaxMemory);
            setUploadFileMaxMemory(num * 1024L * 1024L);
        } catch (Exception r) {
        }
    }

    /**
     * @param uploadFileMaxMemory the uploadBlockThreadNum to set
     */
    public void setUploadFileMaxMemory(long uploadFileMaxMemory) {
        if (uploadFileMaxMemory < 1024L * 1024L * 2L) {
            uploadFileMaxMemory = 1024L * 1024L * 2L;
        }
        if (uploadFileMaxMemory > 1024L * 1024L * 20L) {
            uploadFileMaxMemory = 1024L * 1024L * 20L;
        }
        this.uploadFileMaxMemory = uploadFileMaxMemory;
    }

    /**
     * @return the uploadShardThreadNum
     */
    public int getUploadShardThreadNum() {
        return uploadShardThreadNum;
    }

    public void setUploadShardThreadNum(String uploadShardThreadNum) {
        try {
            int num = Integer.parseInt(uploadShardThreadNum);
            setUploadShardThreadNum(num);
        } catch (Exception r) {
        }
    }

    /**
     * @param uploadShardThreadNum the uploadShardThreadNum to set
     */
    public void setUploadShardThreadNum(int uploadShardThreadNum) {
        if (uploadShardThreadNum < 50) {
            uploadShardThreadNum = 50;
        }
        if (uploadShardThreadNum > 3000) {
            uploadShardThreadNum = 3000;
        }
        this.uploadShardThreadNum = uploadShardThreadNum;
    }

    /**
     * @return the PNN
     */
    public int getPNN() {
        return PNN;
    }

    public void setPNN(String PNN) {
        try {
            int num = Integer.parseInt(PNN);
            setPNN(num);
        } catch (Exception r) {
        }
    }

    /**
     * @param PNN the PNN to set
     */
    public void setPNN(int PNN) {
        if (PNN < 320) {
            PNN = 320;
        }
        if (PNN > 1000) {
            PNN = 1000;
        }
        this.PNN = PNN;
    }

    /**
     * @return the PTR
     */
    public int getPTR() {
        return PTR;
    }

    public void setPTR(String PTR) {
        try {
            int num = Integer.parseInt(PTR);
            setPTR(num);
        } catch (Exception r) {
        }
    }

    /**
     * @param PTR the PTR to set
     */
    public void setPTR(int PTR) {
        if (PTR < 1) {
            PTR = 1;
        }
        if (PTR > 10) {
            PTR = 10;
        }
        this.PTR = PTR * 1000 * 60;
    }

    /**
     * @return the RETRYTIMES
     */
    public int getRETRYTIMES() {
        return RETRYTIMES;
    }

    public void setRETRYTIMES(String RETRYTIMES) {
        try {
            int num = Integer.parseInt(RETRYTIMES);
            setRETRYTIMES(num);
        } catch (Exception r) {
        }
    }

    /**
     * @param RETRYTIMES the RETRYTIMES to set
     */
    public void setRETRYTIMES(int RETRYTIMES) {
        if (RETRYTIMES < 5) {
            RETRYTIMES = 5;
        }
        if (RETRYTIMES > 3000) {
            RETRYTIMES = 3000;
        }
        this.RETRYTIMES = RETRYTIMES;
    }

    /**
     * @return the zipkinServer
     */
    public String getZipkinServer() {
        return zipkinServer;
    }

    /**
     * @param zipkinServer the zipkinServer to set
     */
    public void setZipkinServer(String zipkinServer) {
        this.zipkinServer = zipkinServer;
    }

    /**
     * @return the uploadBlockThreadNum
     */
    public int getUploadBlockThreadNum() {
        return uploadBlockThreadNum;
    }

}
