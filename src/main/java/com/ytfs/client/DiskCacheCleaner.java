package com.ytfs.client;

import java.io.File;

public class DiskCacheCleaner extends Thread {

    private static final long expiredTime = 1000 * 60 * 60 * 24 * 3;

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

    private void deleteDirectory(File file) {
        File[] files = file.listFiles();
        for (File f : files) {
            f.delete();
        }
        file.delete();
    }
}
