package com.ytfs.client.examples;

import com.ytfs.client.ClientInitor;
import com.ytfs.client.PreAllocNodeStat;
import com.ytfs.client.PreAllocNodes;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.ShardEncoder;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import com.ytfs.service.packet.UploadShard2CResp;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.service.packet.UploadShardRes;
import com.ytfs.service.packet.node.GetNodeCapacityReq;
import com.ytfs.service.packet.node.GetNodeCapacityResp;
import io.yottachain.nodemgmt.core.vo.Node;
import io.yottachain.p2phost.utils.Base58;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ChouCha {

    private static final Logger LOG = Logger.getLogger(ChouCha.class);

    public static void main(String[] args) throws Exception {
        System.setProperty("snlist.conf", "conf/snlist.properties");
        System.setProperty("ytfs.conf", "conf/ytfs.properties");

        InputStream is = ChouCha.class.getResourceAsStream("list.properties");
        Properties p = new Properties();
        p.load(is);
        is.close();
        Collection<Object> ids = p.values();
        List<Integer> idlist = new ArrayList();
        for (Object obj : ids) {
            try {
                int id = Integer.parseInt(obj.toString());
                if (!idlist.contains(id)) {
                    idlist.add(id);
                }
            } catch (Exception r) {
            }
        }
        idlist.clear();
        idlist.add(21);
        ClientInitor.init();

        Map<Integer, PreAllocNodeStat> oklist = new HashMap();

        while (true) {
            List<PreAllocNodeStat> ls = PreAllocNodes.getNodes();
            for (PreAllocNodeStat stat : ls) {
                if (idlist.contains(stat.getId())) {
                    oklist.put(stat.getId(), stat);
                }
            }
            if (oklist.size() < 1) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {

                }
            } else {
                break;
            }
        }

        PreAllocNodeStat st = oklist.get(21);
        byte[] data = MakeRandFile.makeMediumFile();

        byte[] VHF = upload(data, st);

        if (VHF != null) {
            while (true) {
                try {
                    boolean b = download(VHF, st);
                    LOG.info("Verify:" + b);
                } catch (Exception e) {
                    LOG.info("Verify ERR:" + e.getMessage());
                }
                Thread.sleep(1000*5);
            }
        }

        /*
        Set<Map.Entry<Object, Object>> set = p.entrySet();
        for (Map.Entry<Object, Object> ent : set) {
            try {
                int id = Integer.parseInt(ent.getValue().toString());
                if (!oklist.containsKey(id)) {
                    continue;
                }
                String hash = ent.getKey().toString();
                byte[] bs = Base64.decode(hash);
                byte[] vhf = new byte[16];
                System.arraycopy(bs, bs.length - 16, vhf, 0, 16);
                LOG.info("Verify:" + hash+",base58:"+Base58.encode(vhf));               
                try {
                    boolean b = download(vhf, oklist.get(id));
                    LOG.info("Verify:" + b);
                } catch (Exception e) {
                    LOG.info("Verify ERR:" + e.getMessage());
                }
            } catch (Exception r) {
            }
        }*/
    }

    private static byte[] upload(byte[] data, PreAllocNodeStat st) throws ServiceException {
        byte[] VHF = ShardEncoder.sha(data);
        UploadShardReq req = new UploadShardReq();
        req.setBPDID(UserConfig.superNode.getId());
        req.setBPDSIGN(st.getSign().getBytes());
        req.setUSERSIGN("XXXX".getBytes());
        req.setDAT(data);
        req.setSHARDID(1);
        req.setVHF(VHF);

        GetNodeCapacityReq ctlreq = new GetNodeCapacityReq();
        ctlreq.setRetryTimes(0);
        ctlreq.setStartTime(System.currentTimeMillis());
        GetNodeCapacityResp ctlresp = (GetNodeCapacityResp) P2PUtils.requestNode(ctlreq, st.getNode(), "");
        req.setAllocId(ctlresp.getAllocId());
        UploadShard2CResp resp = (UploadShard2CResp) P2PUtils.requestNode(req, st.getNode(), "");
        if (resp.getRES() == UploadShardRes.RES_OK || resp.getRES() == UploadShardRes.RES_VNF_EXISTS) {
            LOG.info("upload OK:"+Base58.encode(VHF));
            return VHF;
        } else {
            return null;
        }
    }

    private static boolean download(byte[] vhf, PreAllocNodeStat stat) throws ServiceException {
        DownloadShardReq req = new DownloadShardReq();
        req.setVHF(vhf);
        Node node = stat.getNode();
        DownloadShardResp resp = (DownloadShardResp) P2PUtils.requestNode(req, node);
        return verify(resp, vhf);
    }

    static boolean verify(DownloadShardResp resp, byte[] VHF) {
        byte[] data = resp.getData();
        if (data == null) {
            LOG.error("Return data length:" + (data == null ? "0" : data.length));
            return false;
        }
        if (data.length < UserConfig.Default_Shard_Size) {
            LOG.info("Return data length:" + (data == null ? "0" : data.length));
            return false;
        } else if (data.length == UserConfig.Default_Shard_Size) {
        } else {
            LOG.info("Return data length:" + (data == null ? "0" : data.length));
            byte[] bs = new byte[UserConfig.Default_Shard_Size];
            System.arraycopy(data, 0, bs, 0, bs.length);
            resp.setData(bs);
        }
        try {
            MessageDigest sha256 = MessageDigest.getInstance("MD5");
            byte[] bs = sha256.digest(resp.getData());
            return Arrays.equals(bs, VHF);
        } catch (NoSuchAlgorithmException ex) {
            return false;
        }
    }
}
