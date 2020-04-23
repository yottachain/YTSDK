package com.ytfs.client.batch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;
import org.tanukisoftware.wrapper.WrapperManager;

public class MakeFile {

    static int UPLOAD_FILE_LEN = 100;
    static boolean UPLOAD_FILE_FIX_LEN = true;
    static boolean UPLOAD_DATA_REPEAT = true;

    static {
        String num = WrapperManager.getProperties().getProperty("wrapper.batch.uploadFileLength", "10");
        try {
            UPLOAD_FILE_LEN = Integer.parseInt(num);
        } catch (Exception d) {
        }
        num = WrapperManager.getProperties().getProperty("wrapper.batch.uploadFileFixSize", "true");
        if (num.equalsIgnoreCase("false")) {
            UPLOAD_FILE_FIX_LEN = false;
        }
        num = WrapperManager.getProperties().getProperty("wrapper.batch.uploadRepeatData", "true");
        if (num.equalsIgnoreCase("false")) {
            UPLOAD_DATA_REPEAT = false;
        }
    }
    private final String filepath;

    public MakeFile(String dirpath, String bucketNum) {
        this.filepath = dirpath + File.separator + bucketNum + ".dat";
    }

    /**
     * @return the filepath
     */
    public String getFilePath() {
        return filepath;
    }

    private byte[] makeSmallBytes() {
        int length = 1024 * 4;
        Random ran = new Random(System.currentTimeMillis());
        ByteBuffer buf = ByteBuffer.allocate(length);
        for (int ii = 0; ii < length / 8; ii++) {
            long l = ran.nextLong();
            buf.putLong(l);
        }
        return buf.array();
    }

    private byte[] makeBytes() {
        int length = 1024 * 1024;
        Random ran = new Random(System.currentTimeMillis());
        ByteBuffer buf = ByteBuffer.allocate(length);
        for (int ii = 0; ii < length / 8; ii++) {
            long l = ran.nextLong();
            buf.putLong(l);
        }
        return buf.array();
    }

    private int makeLength() {
        return makeLength(UPLOAD_FILE_FIX_LEN);
    }

    private int makeLength(boolean fix) {
        if (fix) {
            return UPLOAD_FILE_LEN;
        } else {
            Random ran = new Random();
            int num = UPLOAD_FILE_LEN / 5;
            if (num == 0) {
                return UPLOAD_FILE_LEN;
            }
            int ii = ran.nextInt(num);
            return num * 4 + ii;
        }
    }

    public long makeFile() throws IOException {
        return makeFile(false);
    }

    public long makeFile(boolean repeat) throws IOException {
        int loopnum = makeLength();
        long needlength = 1024L * 1024L * (long) loopnum;
        File file = new File(getFilePath());
        if (repeat) {
            if (file.exists()) {
                return file.length();
            }
        }
        if (file.exists()) {
            long length = file.length();
            if (UPLOAD_DATA_REPEAT) {
                loopnum = 2;
                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(file, true);
                    for (long ii = 0; ii < loopnum; ii++) {
                        byte[] bs = makeBytes();
                        fos.write(bs);
                    }
                } finally {
                    if (fos != null) {
                        fos.close();
                    }
                }
                return length + 1024L * 1024L * (long) loopnum;
            } else {
                RandomAccessFile raf = null;
                try {
                    if (needlength > length) {
                        raf = new RandomAccessFile(file, "rw");
                        long num = length / 1024L / 1024L;
                        long skipn = 0;
                        for (long ii = 0; ii < num; ii++) {
                            raf.seek(skipn);
                            byte[] bs = makeSmallBytes();
                            raf.write(bs);
                            skipn = skipn + 1024L * 1024L;
                        }
                        raf.close();
                        raf = null;
                        long loop = (needlength - length) / 1024L / 1024L;
                        if (loop > 0) {
                            FileOutputStream fos = null;
                            try {
                                fos = new FileOutputStream(file, true);
                                for (long ii = 0; ii < loop; ii++) {
                                    byte[] bs = makeBytes();
                                    fos.write(bs);
                                }
                            } finally {
                                if (fos != null) {
                                    fos.close();
                                }
                            }
                        }
                    } else {
                        raf = new RandomAccessFile(file, "rw");
                        raf.setLength(needlength);
                        long skipn = 0;
                        long num = loopnum;
                        for (long ii = 0; ii < num; ii++) {
                            raf.seek(skipn);
                            byte[] bs = makeSmallBytes();
                            raf.write(bs);
                            skipn = skipn + 1024L * 1024L;
                        }
                    }
                } finally {
                    if (raf != null) {
                        raf.close();
                    }
                }
            }
        } else {
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(file);
                for (int ii = 0; ii < loopnum; ii++) {
                    byte[] bs = makeBytes();
                    fos.write(bs);
                }
            } finally {
                if (fos != null) {
                    fos.close();
                }
            }
        }
        return needlength;
    }

}
