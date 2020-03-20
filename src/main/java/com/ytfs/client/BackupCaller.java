package com.ytfs.client;

import java.io.IOException;
import java.io.InputStream;

public interface BackupCaller {

    public byte[] getAESKey();

    public InputStream getBackupInputStream(long start) throws IOException;

}
