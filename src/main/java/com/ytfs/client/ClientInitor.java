package com.ytfs.client;

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
        reguser();
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
                LOG.info("P2P initialization completed, port " + port);
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
    private static void reguser() throws IOException {
        Exception err = null;
        for (int ii = 0; ii < 10; ii++) {
            try {
                RegUser.regist();
                LOG.info("User Registration Successful.");
                err = null;
                break;
            } catch (Exception r) {
                LOG.info("User registration failed.", r);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                }
                err = r;
            }
        }
        if (err != null) {
            throw new IOException(err);
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
        superNode = new SuperNode(0, null, null, null, null);
        superNode.setNodeid(cfg.getSuperNodeID());
        superNode.setAddrs(cfg.getSuperNodeAddrs());
        exportPrivateKey(cfg.getKUSp());
        username = cfg.getUsername();
        contractAccount = cfg.getContractAccount();
        tmpFilePath = new File(cfg.getTmpFilePath(), "ytfs.temp");
        if (!tmpFilePath.exists()) {
            tmpFilePath.mkdirs();
        }
        if (!tmpFilePath.isDirectory()) {
            throw new IOException("The 'tmpFilePath' parameter is not configured.");
        }
    }

    private static void exportPrivateKey(String password) throws IOException {
        try {
            KUSp = Base58.decode(password);
            if (KUSp.length != 37) {
                throw new Exception();
            }
            privateKey = password;
            AESKey = KeyStoreCoder.generateRandomKey(KUSp);
        } catch (Throwable r) {
            String path = System.getProperty("ytfs.cert", "../conf/cert");
            privateKey = ReadPrivateKey.getPrivateKey(path, password);
            if (privateKey == null) {
                throw new IOException();
            }
            KUSp = Base58.decode(privateKey);
            AESKey = KeyStoreCoder.generateRandomKey(KUSp);
        }
    }

    /**
     * 从本地配置文件ytfs.properties加载配置
     *
     * @throws IOException
     */
    private static void load() throws IOException {
        String path = System.getProperty("ytfs.conf", "../conf/ytfs.properties");
        InputStream is = null;
        try {
            is = new FileInputStream(path);
        } catch (Exception r) {
            is = ClientInitor.class.getResourceAsStream("/ytfs.properties");
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
        contractAccount = p.getProperty("contractAccount");
        if (contractAccount == null || contractAccount.trim().isEmpty()) {
            throw new IOException("The 'contractAccount' parameter is not configured.");
        }
        superNode = new SuperNode(0, null, null, null, null);
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
