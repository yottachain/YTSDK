package com.ytfs.client.v2;

import static com.ytfs.client.ClientInitor.inited;
import static com.ytfs.client.ClientInitor.startP2p;
import com.ytfs.client.Configurator;
import com.ytfs.client.DownloadShard;
import com.ytfs.client.PreAllocNodes;
import com.ytfs.client.RegUser;
import com.ytfs.client.UploadBlockExecuter;
import com.ytfs.client.UploadShard;
import com.ytfs.common.LogConfigurator;
import com.ytfs.common.codec.lrc.MemoryCache;
import static com.ytfs.common.conf.UserConfig.DOWNLOADSHARDTHREAD;
import static com.ytfs.common.conf.UserConfig.PNN;
import static com.ytfs.common.conf.UserConfig.PTR;
import static com.ytfs.common.conf.UserConfig.RETRYTIMES;
import static com.ytfs.common.conf.UserConfig.UPLOADBLOCKTHREAD;
import static com.ytfs.common.conf.UserConfig.UPLOADFILEMAXMEMORY;
import static com.ytfs.common.conf.UserConfig.UPLOADSHARDTHREAD;
import static com.ytfs.common.conf.UserConfig.tmpFilePath;
import static com.ytfs.common.conf.UserConfig.zipkinServer;
import com.ytfs.common.tracing.GlobalTracer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.tanukisoftware.wrapper.WrapperManager;

public class YTClientMgr {

    private static final Logger LOG = Logger.getLogger(YTClientMgr.class);
    private static final Map<String, YTClient> clients = new ConcurrentHashMap<>();

    public static YTClient newInstance(String username, String privateKey) throws IOException {
        if (clients.size() > 2000) {
            throw new IOException("Maximum number of users reached.");
        }
        YTClient client = new YTClient(username, privateKey);
        RegUser.regist(client);
        putClient(client);
        return client;
    }

    private static void putClient(YTClient client) {
        clients.put(client.getUserId() + client.getPrivateKey(), client);
    }

    public static YTClient getClient(int userid, String key) {
        return clients.get(userid + key);
    }

    public static void free(YTClient client) {
        clients.remove(client.getUserId() + client.getPrivateKey());
    }

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
    public synchronized static void init(Configurator cfg) throws IOException {
        String level = WrapperManager.getProperties().getProperty("wrapper.log4j.loglevel", "DEBUG");
        String path = WrapperManager.getProperties().getProperty("wrapper.log4j.logfile");
        if (!inited) {
            LogConfigurator.configPath(path == null ? null : new File(path), level);
            if (cfg == null) {
                load();
            } else {
                load(cfg);
            }
            UploadBlockExecuter.init();
            UploadShard.init();
            DownloadShard.init();
            startP2p();
            PreAllocNodes.init();
            MemoryCache.init();
            GlobalTracer.init(zipkinServer, "S3server");
            inited = true;
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
        cfg.setUploadBlockThreadNum(p.getProperty("uploadBlockThreadNum"));
        cfg.setPNN(p.getProperty("PNN"));
        cfg.setPTR(p.getProperty("PTR"));
        cfg.setRETRYTIMES(p.getProperty("RETRYTIMES"));
        DOWNLOADSHARDTHREAD = cfg.getDownloadThread();
        UPLOADFILEMAXMEMORY = cfg.getUploadFileMaxMemory();
        UPLOADSHARDTHREAD = cfg.getUploadShardThreadNum();
        UPLOADBLOCKTHREAD = cfg.getUploadBlockThreadNum();
        PNN = cfg.getPNN();
        PTR = cfg.getPTR();
        RETRYTIMES = cfg.getRETRYTIMES();
        zipkinServer = p.getProperty("zipkinServer");
    }

    /**
     * 从Configurator加载配置
     *
     * @param cfg
     * @throws IOException
     */
    private static void load(Configurator cfg) throws IOException {
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
        UPLOADBLOCKTHREAD = cfg.getUploadBlockThreadNum();
        PNN = cfg.getPNN();
        PTR = cfg.getPTR();
        RETRYTIMES = cfg.getRETRYTIMES();
        zipkinServer = cfg.getZipkinServer();
    }
}
