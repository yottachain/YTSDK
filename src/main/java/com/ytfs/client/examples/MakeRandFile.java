package com.ytfs.client.examples;

import java.nio.ByteBuffer;
import java.util.Random;

public class MakeRandFile {

    private static final int smallFileLength = 128;
    private static final int mediumFileLength = 1024 * 8;
    private static final int largeFileLength = 1024 * 1024 * 2 - 1024 * 16;

    public static byte[] makeBytes(int length) {
        Random ran = new Random();
        ByteBuffer buf = ByteBuffer.allocate(length);
        for (int ii = 0; ii < length / 8; ii++) {
            long l = ran.nextLong();
            buf.putLong(l);
        }
        return buf.array();
    }

    public static byte[] makeSmallFile() {
        return makeBytes(smallFileLength);
    }

    public static byte[] makeMediumFile() {
        return makeBytes(mediumFileLength);
    }

    public static byte[] makeLargeFile() {
        return makeBytes(largeFileLength);
    }
}
