package com.ytfs.client;

import static com.ytfs.common.conf.UserConfig.*;
import com.ytfs.common.net.P2PUtils;
import com.ytfs.common.GlobleThreadPool;
import com.ytfs.common.LogConfigurator;
import com.ytfs.common.ServiceErrorCode;
import com.ytfs.common.ServiceException;
import com.ytfs.common.codec.KeyStoreCoder;
import com.ytfs.common.codec.ReadPrivateKey;
import com.ytfs.common.codec.lrc.MemoryCache;
import com.ytfs.common.conf.UserConfig;
import com.ytfs.common.net.LoginCaller;
import com.ytfs.common.tracing.GlobalTracer;
import com.ytfs.service.packet.user.LoginReq;
import io.jafka.jeos.util.Base58;
import io.jafka.jeos.util.KeyUtil;
import io.yottachain.nodemgmt.core.vo.SuperNode;
import io.yottachain.ytcrypto.core.exception.YTCryptoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperManager;

public class ClientInitor {

    private static final Logger LOG = Logger.getLogger(ClientInitor.class);

    /**
     * 初始化SDK
     *
     * @throws IOException
     */
    public static void init() throws IOException {
        init(null);
    }

    private static boolean inited = false;

    /**
     * 初始化SDK
     *
     * @param cfg
     * @throws IOException
     */
    public static void init(Configurator cfg) throws IOException {
        String level = WrapperManager.getProperties().getProperty("wrapper.log4j.loglevel", "DEBUG");
        String path = WrapperManager.getProperties().getProperty("wrapper.log4j.logfile");
        if (!inited) {
            LogConfigurator.configPath(path == null ? null : new File(path), level);
            if (cfg == null) {
                load();
            } else {
                load(cfg);
            }
            startP2p();
            RegUser.regist();
            regCaller();
            PreAllocNodes.init();
            MemoryCache.init();
            GlobalTracer.init(zipkinServer, "S3server");
            inited = true;
        } else {
            LogConfigurator.configPath(path == null ? null : new File(path), level);
            if (cfg == null) {
                load();
            } else {
                load(cfg);
            }
            RegUser.regist();
            regCaller();
        }
    }

    private static void regCaller() {
        LoginCaller caller = (SuperNode node) -> {
            LoginReq req = new LoginReq();
            req.setUserId(userId);
            req.setKeyNumber(keyNumber);
            String data = userId + username + keyNumber;
            byte[] signdata = data.getBytes(Charset.forName("UTF-8"));
            String sign = null;
            try {
                sign = io.yottachain.ytcrypto.YTCrypto.sign(privateKey, signdata);
            } catch (YTCryptoException ex) {
                LOG.error("Sign ERR:" + ex.getMessage());
            }
            req.setSignData(sign);
            try {
                P2PUtils.requestBPU(req, node, UserConfig.SN_RETRYTIMES);
            } catch (ServiceException ex) {
                if (ex.getErrorCode() == ServiceErrorCode.INVALID_USER_ID) {
                    try {
                        Thread.sleep(30000);
                    } catch (InterruptedException ex1) {
                    }
                }
            }
        };
        P2PUtils.regLoginCaller(caller);
    }

    /**
     * 初始化p2p网络
     *
     * @throws IOException
     */
    private static void startP2p() throws IOException {
        String randPrivateKey = KeyUtil.createPrivateKey();
        Exception err = null;
        for (int ii = 0; ii < 10; ii++) {
            try {
                int port = freePort();
                P2PUtils.start(port, randPrivateKey);
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
     * 关闭用户端
     */
    public static void stop() {
        PreAllocNodeMgr.shutdown();
        P2PUtils.stop();
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
        UPLOADFILEMAXMEMORY = cfg.getUploadFileMaxMemory();
        UPLOADSHARDTHREAD = cfg.getUploadShardThreadNum();
        PNN = cfg.getPNN();
        PTR = cfg.getPTR();
        RETRYTIMES = cfg.getRETRYTIMES();
        zipkinServer = cfg.getZipkinServer();
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
        cfg.setUploadFileMaxMemory(p.getProperty("uploadFileMaxMemory"));
        cfg.setUploadShardThreadNum(p.getProperty("uploadShardThreadNum"));
        cfg.setPNN(p.getProperty("PNN"));
        cfg.setPTR(p.getProperty("PTR"));
        cfg.setRETRYTIMES(p.getProperty("RETRYTIMES"));
        DOWNLOADSHARDTHREAD = cfg.getDownloadThread();
        UPLOADFILEMAXMEMORY = cfg.getUploadFileMaxMemory();
        UPLOADSHARDTHREAD = cfg.getUploadShardThreadNum();
        PNN = cfg.getPNN();
        PTR = cfg.getPTR();
        RETRYTIMES = cfg.getRETRYTIMES();
        zipkinServer = p.getProperty("zipkinServer");
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
