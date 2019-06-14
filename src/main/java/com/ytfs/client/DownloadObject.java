package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.ServiceException;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.DownloadObjectInitReq;
import com.ytfs.service.packet.DownloadObjectInitResp;
import com.ytfs.service.packet.ObjectRefer;
import com.ytfs.service.packet.s3.DownloadFileReq;
import org.bson.types.ObjectId;

import java.io.InputStream;
import java.util.List;

public class DownloadObject {

    private byte[] VHW;
    private List<ObjectRefer> refers;
    private long length;

    public DownloadObject(byte[] VHW) throws ServiceException {
        this.VHW = VHW;
        init();
    }

    public DownloadObject(String bucketName, String fileName,ObjectId versionId) throws ServiceException {
        init(bucketName, fileName,versionId);
    }

    private void init() throws ServiceException {
        DownloadObjectInitReq req = new DownloadObjectInitReq();
        req.setVHW(VHW);
        DownloadObjectInitResp resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        refers = ObjectRefer.parse(resp.getRefers());
        this.length = resp.getLength();
    }

    private void init(String bucketName, String fileName, ObjectId versionId) throws ServiceException {
        DownloadFileReq req = new DownloadFileReq();
        req.setBucketname(bucketName);
        req.setFileName(fileName);
        System.out.println("versionId=====" + versionId);
        if(versionId != null) {
            req.setVersionId(versionId);
        }
        DownloadObjectInitResp resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        refers = ObjectRefer.parse(resp.getRefers());
        this.length = resp.getLength();
    }

    public InputStream load() {
        return new DownloadInputStream(refers, 0, this.getLength());
    }

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
