package com.ytfs.client;

import com.ytfs.client.v2.YTClient;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.ServiceException;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.DownloadObjectInitReq;
import com.ytfs.service.packet.user.DownloadObjectInitResp;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.s3.DownloadFileReq;
import com.ytfs.service.packet.v2.DownloadFileReqV2;
import com.ytfs.service.packet.v2.DownloadObjectInitReqV2;
import org.bson.types.ObjectId;
import java.io.InputStream;
import java.util.List;

public class DownloadObject {

    private byte[] VHW;
    private List<ObjectRefer> refers;
    private long length;
    private BackupCaller backupCaller = null;
    private YTClient client = null;

    public DownloadObject(YTClient client, byte[] VHW) throws ServiceException {
        this.client = client;
        this.VHW = VHW;
        init();
    }

    public DownloadObject(YTClient client, String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        this.client = client;
        init(bucketName, fileName, versionId);
    }

    /**
     * 创建下载实例
     *
     * @param VHW
     * @throws ServiceException
     */
    public DownloadObject(byte[] VHW) throws ServiceException {
        this.VHW = VHW;
        init();
    }

    /**
     * 创建下载实例
     *
     * @param bucketName
     * @param fileName 文件名
     * @param versionId
     * @throws ServiceException
     */
    public DownloadObject(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        init(bucketName, fileName, versionId);
    }

    private void init() throws ServiceException {
        DownloadObjectInitResp resp;
        if (client == null) {
            DownloadObjectInitReq req = new DownloadObjectInitReq();
            req.setVHW(VHW);
            resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
        } else {
            DownloadObjectInitReqV2 req = new DownloadObjectInitReqV2();
            req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
            req.setVHW(VHW);
            resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, client.getSuperNode(), UserConfig.SN_RETRYTIMES);
        }
        refers = ObjectRefer.parse(resp.getRefers());
        this.length = resp.getLength();
    }

    private void init(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        DownloadObjectInitResp resp;
        if (client == null) {
            DownloadFileReq req = new DownloadFileReq();
            req.setBucketname(bucketName);
            req.setFileName(fileName);
            if (versionId != null) {
                req.setVersionId(versionId);
            }
            resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
        } else {
            DownloadFileReqV2 req = new DownloadFileReqV2();
            req.fill(client.getUserId(), client.getKeyNumber(), client.getPrivateKey());
            req.setBucketname(bucketName);
            req.setFileName(fileName);
            if (versionId != null) {
                req.setVersionId(versionId);
            }
            resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, client.getSuperNode(), UserConfig.SN_RETRYTIMES);
        }
        refers = ObjectRefer.parse(resp.getRefers());
        this.length = resp.getLength();
    }

    /**
     * 开始下载
     *
     * @return
     */
    public InputStream load() {
        return new DownloadInputStream(client, refers, 0, this.getLength(), backupCaller);
    }

    /**
     * 开始下载，按范围下载
     *
     * @param start　开始
     * @param end　结束
     * @return
     */
    public InputStream load(long start, long end) {
        return new DownloadInputStream(client, refers, start, end, backupCaller);
    }

    /**
     * @return the length
     */
    public long getLength() {
        return length;
    }

    /**
     * @return the backupCaller
     */
    public BackupCaller getBackupCaller() {
        return backupCaller;
    }

    /**
     * @param backupCaller the backupCaller to set
     */
    public void setBackupCaller(BackupCaller backupCaller) {
        this.backupCaller = backupCaller;
    }
}
