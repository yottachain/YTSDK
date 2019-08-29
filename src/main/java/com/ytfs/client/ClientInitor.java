package com.ytfs.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ytfs.common.conf.UserConfig;
import static com.ytfs.common.conf.UserConfig.*;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.LogConfigurator;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.ReadPrivateKey;
import io.jafka.jeos.util.Base58;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperManager;

public class ClientInitor {

    private static final Logger LOG = Logger.getLogger(ClientInitor.class);
    private static final DiskCacheCleaner clean = new DiskCacheCleaner();

    /**
     * 初始化SDK
     *
     * @throws IOException
     */
    public static void init() throws IOException {
        init(null);
    }

    /**
     * 初始化SDK
     *
     * @param cfg
     * @throws IOException
     */
    public static void init(Configurator cfg) throws IOException {
        String level = WrapperManager.getProperties().getProperty("wrapper.log4j.loglevel", "DEBUG");
        String path = WrapperManager.getProperties().getProperty("wrapper.log4j.logfile");
        LogConfigurator.configPath(path == null ? null : new File(path), level);
        if (cfg == null) {
            load();
        } else {
            load(cfg);
        }
        startP2p();
        regUser();
        clean.start();
    }

    /**
     * 初始化p2p网络
     *
     * @throws IOException
     */
    private static void startP2p() throws IOException {
        Exception err = null;
        for (int ii = 0; ii < 10; ii++) {
            try {
                int port = freePort();
                P2PUtils.start(port, UserConfig.privateKey);
                err = null;
                break;
            } catch (Exception r) {
                LOG.info("P2P initialization failed!", r);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                }
                P2PUtils.stop();
                err = r;
            }
        }
        if (err != null) {
            throw new IOException(err);
        }
    }

    /**
     * 启动后自动注册用户
     *
     * @throws IOException
     */
    private static void regUser() throws IOException {
        String path = System.getProperty("snlist.conf", "conf/snlist.properties");
        InputStream is = null;
        try {
            is = new FileInputStream(path);
        } catch (Exception r) {
        }
        if (is == null) {
            throw new IOException("No snlist properties file could be found for ytfs service");
        }
        List snlist;
        try {
            ObjectMapper mapper = new ObjectMapper();
            snlist = mapper.readValue(is, ArrayList.class);
        } finally {
            is.close();
        }
        if (snlist == null || snlist.isEmpty()) {
            throw new IOException("No snlist properties file could be found for ytfs service");
        }
        while (true) {
            long index = System.currentTimeMillis() % snlist.size();
            Map map = (Map) snlist.remove((int) index);
            try {
                SuperNode sn = new SuperNode(0, null, null, null, null);
                sn.setId(Integer.parseInt(map.get("Number").toString()));
                sn.setNodeid(map.get("ID").toString());
                List addr = (List) map.get("Addrs");
                sn.setAddrs(addr);
                RegUser.regist(sn);
                LOG.info("User Registration Successful.");
                return;
            } catch (Throwable r) {
                LOG.info("User registration failed:" + r.getMessage());
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException ex) {
                }
                if (snlist.isEmpty()) {
                    throw new IOException(r);
                }
            }
        }
    }

    /**
     * 关闭用户端
     */
    public static void stop() {
        P2PUtils.stop();
        clean.interrupt();
        GlobleThreadPool.shutdown();
    }

    /**
     * 从Configurator加载配置
     *
     * @param cfg
     * @throws IOException
     */
    private static void load(Configurator cfg) throws IOException {
        exportPrivateKey(cfg.getKUSp());
        username = cfg.getUsername();
        tmpFilePath = new File(cfg.getTmpFilePath(), "ytfs.temp");
        if (!tmpFilePath.exists()) {
            tmpFilePath.mkdirs();
        }
        if (!tmpFilePath.isDirectory()) {
            throw new IOException("The 'tmpFilePath' parameter is not configured.");
        }
        DOWNLOADSHARDTHREAD = cfg.getDownloadThread();
        UPLOADBLOCKTHREAD = cfg.getUploadBlockThreadNum();
        UPLOADSHARDTHREAD = cfg.getUploadShardThreadNum();
    }

    private static void exportPrivateKey(String password) throws IOException {
        try {
            KUSp = Base58.decode(password);
            if (KUSp.length != 37) {
                throw new Exception();
            }
            privateKey = password;
            AESKey = KeyStoreCoder.generateUserKey(KUSp);
        } catch (Throwable r) {
            String path = System.getProperty("ytfs.cert", "../conf/cert");
            privateKey = ReadPrivateKey.getPrivateKey(path, password);
            if (privateKey == null) {
                throw new IOException();
            }
            KUSp = Base58.decode(privateKey);
            AESKey = KeyStoreCoder.generateUserKey(KUSp);
        }
    }

    /**
     * 从本地配置文件ytfs.properties加载配置
     *
     * @throws IOException
     */
    private static void load() throws IOException {
        String path = System.getProperty("ytfs.conf", "conf/ytfs.properties");
        InputStream is = null;
        try {
            is = new FileInputStream(path);
        } catch (Exception r) {
        }
        if (is == null) {
            throw new IOException("No properties file could be found for ytfs service");
        }
        Properties p = new Properties();
        p.load(is);
        is.close();
        username = p.getProperty("username");
        if (username == null || username.trim().isEmpty()) {
            throw new IOException("The 'username' parameter is not configured.");
        }
        exportPrivateKey(p.getProperty("KUSp").trim());
        try {
            String ss = p.getProperty("tmpFilePath", "").trim();
            File parent = new File(ss);
            tmpFilePath = new File(parent.getAbsolutePath(), "ytfs.temp");
            if (!tmpFilePath.exists()) {
                tmpFilePath.mkdirs();
            }
        } catch (Exception d) {
            throw new IOException("The 'tmpFilePath' parameter is not configured.");
        }
        if (!tmpFilePath.isDirectory()) {
            throw new IOException("The 'tmpFilePath' parameter is not configured.");
        }
        Configurator cfg = new Configurator();
        cfg.setDownloadThread(p.getProperty("downloadThread"));
        cfg.setUploadBlockThreadNum(p.getProperty("uploadBlockThreadNum"));
        cfg.setUploadShardThreadNum(p.getProperty("uploadShardThreadNum"));
        DOWNLOADSHARDTHREAD = cfg.getDownloadThread();
        UPLOADBLOCKTHREAD = cfg.getUploadBlockThreadNum();
        UPLOADSHARDTHREAD = cfg.getUploadShardThreadNum();
    }

    /**
     * 监听本地随机端口以接收数据
     *
     * @return int
     * @throws IOException
     */
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
}
