package com.ytfs.client;

import com.ytfs.service.UserConfig;
import com.ytfs.service.utils.LogConfigurator;
import static com.ytfs.service.UserConfig.*;
import com.ytfs.service.net.P2PUtils;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ClientInitor {

    private static final Logger LOG = Logger.getLogger(ClientInitor.class);

    public static void init() {
        try {
            LogConfigurator.configPath(new File("logs"), "INFO");
            load();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);//循环初始化
        }
        String key = Base58.encode(UserConfig.KUSp);
        for (int ii = 0; ii < 10; ii++) {
            try {
                int port = UserConfig.port + ii;
                P2PUtils.start(port, key);
                LOG.info("P2P initialization completed, port " + port);
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
    }

    private static void load() throws IOException {
        InputStream is = ClientInitor.class.getResourceAsStream("/ytfs.properties");
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
            String ss = p.getProperty("secretKey").trim();
            secretKey = Base58.decode(ss);
        } catch (Exception d) {
            throw new IOException("The 'secretKey' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("KUEp").trim();
            KUEp = Base58.decode(ss);
        } catch (Exception d) {
            throw new IOException("The 'KUEp' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("KUSp").trim();
            KUSp = Base58.decode(ss);
        } catch (Exception d) {
            throw new IOException("The 'KUSp' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("port").trim();
            port = Integer.parseInt(ss);
        } catch (Exception d) {
            throw new IOException("The 'port' parameter is not configured.");
        }
    }
}
