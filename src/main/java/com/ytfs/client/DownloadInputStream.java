package com.ytfs.client;

import com.ytfs.common.codec.AESDecryptInputStream;
import com.ytfs.common.codec.Block;
import com.ytfs.common.codec.BlockInputStream;
import com.ytfs.service.packet.ObjectRefer;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DownloadInputStream extends InputStream {

    private Map<Integer, ObjectRefer> refers = new HashMap();
    private final BackupCaller backupCaller;
    private final long end;
    private InputStream bin;
    private long readpos;
    private long pos = 0;
    private int referIndex = 0;

    public DownloadInputStream(List<ObjectRefer> refs, long start, long end, BackupCaller backupCaller) {
        refs.stream().forEach((refer) -> {
            int id = refer.getId() & 0xFFFF;
            this.refers.put(id, refer);
        });
        this.readpos = start;
        this.end = end;
        this.backupCaller = backupCaller;
    }

    private void readBlock() throws IOException {
        if (bin != null && bin instanceof BlockInputStream) {
            bin = null;
        }
        while (bin == null) {
            ObjectRefer refer = refers.get(referIndex);
            if (refer == null) {
                return;
            }
            if (readpos < pos + refer.getOriginalSize()) {
                try {
                    DownloadBlock db = new DownloadBlock(refer);
                    db.load();
                    Block block = new Block(db.getData());
                    bin = new BlockInputStream(block);
                    long skip = pos - readpos;
                    if (skip > 0) {
                        bin.skip(skip);
                    }
                } catch (Throwable e) {
                    if (backupCaller == null) {
                        throw e instanceof IOException ? (IOException) e : new IOException(e);
                    } else {
                        long startpos = readpos / 16;
                        int skipn = (int) (readpos % 16L);
                        InputStream is = backupCaller.getBackupInputStream(startpos);
                        bin = new AESDecryptInputStream(is, backupCaller.getAESKey());
                        if (skipn > 0) {
                            bin.skip(skipn);
                        }
                    }
                }
            }
            pos = pos + refer.getOriginalSize();
            referIndex++;
        }
    }

    @Override
    public void close() throws IOException {
        refers = null;
        if (bin != null && bin instanceof AESDecryptInputStream) {
            bin.close();
        }
    }

    @Override
    public int read() throws IOException {
        if (refers == null) {
            throw new IOException("Stream closed");
        }
        if (end == readpos) {
            return -1;
        }
        if (bin == null) {
            readBlock();
            if (bin == null) {
                return -1;
            }
        }
        int r = bin.read();
        if (r == -1) {
            if (bin instanceof AESDecryptInputStream) {
                return -1;
            }
            readBlock();
            if (bin == null) {
                return -1;
            } else {
                r = bin.read();
                if (r != -1) {
                    readpos++;
                }
            }
        } else {
            readpos++;
        }
        return r;
    }

}
