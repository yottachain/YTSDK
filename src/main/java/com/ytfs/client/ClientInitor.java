package com.ytfs.client;

import com.ytfs.service.UserConfig;
import static com.ytfs.service.UserConfig.*;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.utils.GlobleThreadPool;
import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ClientInitor {

    private static final Logger LOG = Logger.getLogger(ClientInitor.class);
    private static final DiskCacheCleaner clean = new DiskCacheCleaner();

    public static void init(Configurator cfg) throws IOException {
        load(cfg);
        start();
        clean.start();
    }

    private static void start() throws IOException {
        String key = Base58.encode(UserConfig.KUSp);
        boolean ok = false;
        for (int ii = 0; ii < 10; ii++) {
            try {
                int port = UserConfig.port + ii;
                P2PUtils.start(port, key);
                LOG.info("P2P initialization completed, port " + port);
                ok = true;
                break;
            } catch (Exception r) {
                LOG.info("P2P initialization failed!", r);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                }
                P2PUtils.stop();
            }
        }
        if (!ok) {
            throw new IOException();
        }
    }

    public static void init() throws IOException {
        load();
        start();
        clean.start();
    }

    public static void stop() {
        P2PUtils.stop();
        clean.interrupt();
        GlobleThreadPool.shutdown();
    }

    private static void load(Configurator cfg) throws IOException {
        userID = cfg.getUserID();
        superNode = new SuperNode(0, null, null, null, null);
        superNode.setId(cfg.getSuperNodeNum());
        superNode.setNodeid(cfg.getSuperNodeID());
        superNode.setAddrs(cfg.getSuperNodeAddrs());
        KUSp = Base58.decode(cfg.getKUSp());
        privateKey=cfg.getKUSp();
        String pubkey = KeyUtil.toPublicKey(cfg.getKUSp());
        KUEp = Base58.decode(pubkey.substring(3));
        username = cfg.getUsername();
        contractAccount = cfg.getContractAccount();
        port = cfg.getPort();
        tmpFilePath = new File(cfg.getTmpFilePath(), "ytfs.temp");
        if (!tmpFilePath.exists()) {
            tmpFilePath.mkdirs();
        }
        if (!tmpFilePath.isDirectory()) {
            throw new IOException("The 'tmpFilePath' parameter is not configured.");
        }
    }

    private static void load() throws IOException {
        InputStream is = ClientInitor.class.getResourceAsStream("/ytfs.properties");
        if (is == null) {
            try {
                is = new FileInputStream("ytfs.properties");
            } catch (Exception e) {
                throw new IOException("No properties file could be found for ytfs service");
            }
        }
        if (is == null) {
            throw new IOException("No properties file could be found for ytfs service");
        }
        Properties p = new Properties();
        p.load(is);
        is.close();
        try {
            String ss = p.getProperty("userID").trim();
            userID = Integer.parseInt(ss);
        } catch (Exception d) {
            throw new IOException("The 'userID' parameter is not configured.");
        }
        username = p.getProperty("username");
        if (username == null || username.trim().isEmpty()) {
            throw new IOException("The 'username' parameter is not configured.");
        }
        contractAccount = p.getProperty("contractAccount");
        if (contractAccount == null || contractAccount.trim().isEmpty()) {
            throw new IOException("The 'contractAccount' parameter is not configured.");
        }
        superNode = new SuperNode(0, null, null, null, null);
        try {
            String ss = p.getProperty("superNodeID").trim();
            int superNodeID = Integer.parseInt(ss);
            if (superNodeID < 0 || superNodeID > 31) {
                throw new IOException();
            }
            superNode.setId(superNodeID);
        } catch (Exception d) {
            throw new IOException("The 'superNodeID' parameter is not configured.");
        }
        String key = p.getProperty("superNodeKey");
        if (key == null || key.trim().isEmpty()) {
            throw new IOException("The 'superNodeKey' parameter is not configured.");
        }
        superNode.setNodeid(key.trim());
        List<String> ls = new ArrayList();
        for (int ii = 0; ii < 10; ii++) {
            String superNodeAddr = p.getProperty("superNodeAddr" + ii);
            if (superNodeAddr == null || superNodeAddr.trim().isEmpty()) {
            } else {
                ls.add(superNodeAddr.trim());
            }
        }
        if (ls.isEmpty()) {
            throw new IOException("The 'superNodeAddr' parameter is not configured.");
        } else {
            superNode.setAddrs(ls);
        }
        try {
            String ss = p.getProperty("KUSp").trim();
            KUSp = Base58.decode(ss);
            privateKey=ss;
            String pubkey = KeyUtil.toPublicKey(ss);
            KUEp = Base58.decode(pubkey.substring(3));
        } catch (Exception d) {
            throw new IOException("The 'KUSp' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("port").trim();
            port = Integer.parseInt(ss);
        } catch (Exception d) {
            throw new IOException("The 'port' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("tmpFilePath", "").trim();
            tmpFilePath = new File(ss, "ytfs.temp");
            if (!tmpFilePath.exists()) {
                tmpFilePath.mkdirs();
            }
        } catch (Exception d) {
            throw new IOException("The 'tmpFilePath' parameter is not configured.");
        }
        if (!tmpFilePath.isDirectory()) {
            throw new IOException("The 'tmpFilePath' parameter is not configured.");
        }
    }
}
