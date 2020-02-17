package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.ServiceException;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.user.DownloadObjectInitReq;
import com.ytfs.service.packet.user.DownloadObjectInitResp;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.s3.DownloadFileReq;
import org.bson.types.ObjectId;

import java.io.InputStream;
import java.util.List;

public class DownloadObject {

    private byte[] VHW;
    private List<ObjectRefer> refers;
    private long length;

    /**
     * 创建下载实例
     * @param VHW
     * @throws ServiceException 
     */
    public DownloadObject(byte[] VHW) throws ServiceException {
        this.VHW = VHW;
        init();
    }

    /**
     * 创建下载实例
     * @param bucketName
     * @param fileName 文件名
     * @param versionId
     * @throws ServiceException 
     */
    public DownloadObject(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        init(bucketName, fileName, versionId);
    }

    private void init() throws ServiceException {
        DownloadObjectInitReq req = new DownloadObjectInitReq();
        req.setVHW(VHW);
        DownloadObjectInitResp resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
        refers = ObjectRefer.parse(resp.getRefers());
        this.length = resp.getLength();
    }

    private void init(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        DownloadFileReq req = new DownloadFileReq();
        req.setBucketname(bucketName);
        req.setFileName(fileName);
        if (versionId != null) {
            req.setVersionId(versionId);
        }
        DownloadObjectInitResp resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode, UserConfig.SN_RETRYTIMES);
        refers = ObjectRefer.parse(resp.getRefers());
        this.length = resp.getLength();
    }

    /**
     * 开始下载
     * @return 
     */
    public InputStream load() {
        return new DownloadInputStream(refers, 0, this.getLength());
    }

    /**
     * 开始下载，按范围下载
     * @param start　开始
     * @param end　结束
     * @return 
     */
    public InputStream load(long start, long end) {
        return new DownloadInputStream(refers, start, end);
    }

    /**
     * @return the length
     */
    public long getLength() {
        return length;
    }
}
