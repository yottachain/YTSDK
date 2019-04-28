package com.ytfs.client;

import com.ytfs.service.codec.YTFile;
import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException {
        YTFile fragmentor = new YTFile("e:\\mongodb.tgz");
        fragmentor.init("2123142342");
        fragmentor.handle();

       // BlockReader reader = new BlockReader("e:\\2123142342", "d:\\aa.iso");
       // reader.read();
    }
}
