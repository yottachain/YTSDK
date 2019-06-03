package com.ytfs.client;

import com.ytfs.common.conf.UserConfig;
import java.io.File;

public class DiskCacheCleaner extends Thread {

    private static final long expiredTime = 1000 * 60 * 60 * 24 * 3;//最长保留3天

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                clear();
                Thread.sleep(1000 * 60 * 60);
            } catch (InterruptedException ex) {
                break;
            }

        }
    }

    /**
     * 清除本地缓存文件
     */
    private void clear() {
        try {
            File[] files = UserConfig.tmpFilePath.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    long lastModified = getLastModified(file);
                    if (System.currentTimeMillis() - lastModified > expiredTime) {
                        deleteDirectory(file);
                    }
                }

            }
        } catch (Exception r) {
        }
    }

    /**
     * 3天前的文件删掉
     *
     * @param file
     * @return
     */
    private long getLastModified(File file) {
        File[] files = file.listFiles();
        long l = 0;
        for (File f : files) {
            if (f.lastModified() > l) {
                l = f.lastModified();
            }
        }
        return l;
    }

    /**
     * 删除临时目录及文件(压缩加密时产生的缓存)
     *
     * @param file
     */
    private void deleteDirectory(File file) {
        File[] files = file.listFiles();
        for (File f : files) {
            f.delete();
        }
        file.delete();
    }
}
