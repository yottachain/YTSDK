package com.ytfs.client.examples;

import com.ytfs.common.net.P2PUtils;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import com.ytfs.service.packet.UploadShard2CResp;
import com.ytfs.service.packet.UploadShardReq;
import io.yottachain.nodemgmt.core.vo.Node;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.binary.Hex;

public class SendShardTest {

    static Node node;
 
    public static void main(String[] args) throws Exception {
        start();
        node = new Node();
        List<String> ls = new ArrayList();
        ls.add("/ip4/110.253.175.192/tcp/1510");

        node.setAddrs(ls);
        node.setNodeid("16Uiu2HAmKEgs1jy4mkpDFSyALf2yxeQ47KuHpZcioTikTffBxjMC");
        for (int ii = 0; ii < 1; ii++) {
            Sender s = new Sender();
            s.start();
        }
    }

    private static int freePort() throws IOException {
        Socket socket = new Socket();
        try {
            InetSocketAddress inetAddress = new InetSocketAddress(0);
            socket.bind(inetAddress);
            return socket.getLocalPort();
        } finally {
            socket.close();
        }
    }

    private static void start() throws IOException {
        String key = "5JXZV8PBL5Zw87MG7GaBc6jVwzGxPQa7ZfwevN6dqLFpRNPhELw";
        boolean ok = false;
        for (int ii = 0; ii < 1000; ii++) {
            try {
                int port = freePort();
                P2PUtils.start(port, key);
                ok = true;
                break;
            } catch (Exception r) {
                P2PUtils.stop();
            }
        }
        if (!ok) {
            throw new IOException();
        }
    }

    private static UploadShardReq makeUploadShardReq() throws NoSuchAlgorithmException {
        UploadShardReq req = new UploadShardReq();
        byte[] data = MakeRandFile.makeBytes(1024 * 16);
        req.setBPDID(0);
        req.setBPDSIGN(new byte[10]);
        req.setDAT(data);
        req.setSHARDID(0);
        req.setVBI(12345678);
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        req.setVHF(sha.digest(data));
        return req;
    }

    private static class Sender extends Thread {

        @Override
        public void run() {
            while (!this.isInterrupted()) {
                try {
                    UploadShardReq req = makeUploadShardReq();
                    byte[] VHF=req.getVHF();
                    UploadShard2CResp res = (UploadShard2CResp) P2PUtils.requestNode(req, node);  
                    System.out.println(res.getRES());
                    
                    DownloadShardReq dreq=new DownloadShardReq();
                   // byte[] VHF=Hex.decodeHex("87628f3d794b3bd032132b7f5c175a2b076929e22ae7aa2fe5baa14bcdf26585".toCharArray());
                    dreq.setVHF(VHF);
                    DownloadShardResp resp=(DownloadShardResp) P2PUtils.requestNode(dreq, node);  
                    
                    System.out.println(resp.getData().length);
                    return;
                } catch (Exception r) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            }
        }
    }
}
