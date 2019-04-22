package com.ytfs.client;

import com.ytfs.service.packet.ServiceException;

public class UploadTest {

    public static void main(String[] args) {
        try {
            ClientInitor.init();
            UploadObject upload = new UploadObject("sssssssssssssssssaaaaaaaaaaaaaaaaaaa".getBytes());
            upload.upload();

            System.out.println(upload.getVNU());
        } catch (ServiceException se) {
            se.printStackTrace();
            System.out.println(Integer.toHexString(se.getErrorCode()));
        } catch (Exception r) {
            r.printStackTrace();
        }
    }
}
