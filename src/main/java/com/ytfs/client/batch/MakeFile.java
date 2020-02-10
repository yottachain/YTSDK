package com.ytfs.client.batch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

public class MakeFile {

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
        Random ran = new Random();
        while (true) {
            int ii = ran.nextInt(1500);
            if (ii > 50) {
                return ii;
            }
        }
    }

    public void makeFile() throws IOException {
        int loopnum = makeLength();
        File file = new File(getFilePath());
        if (file.exists()) {
            RandomAccessFile raf = null;
            try {
                long length = file.length();
                long needlength = 1024L * 1024L * (long) loopnum;
                if (needlength > length) {
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
                    raf = new RandomAccessFile(file, "rw");
                } else {
                    raf = new RandomAccessFile(file, "rw");
                    raf.setLength(needlength);
                }
                raf.seek(0);
                byte[] bs = makeBytes();
                raf.write(bs);
            } finally {
                if (raf != null) {
                    raf.close();
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
    }

}
