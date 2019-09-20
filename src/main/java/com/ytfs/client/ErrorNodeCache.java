package com.ytfs.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ytfs.common.conf.UserConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ErrorNodeCache {

    private static final long EXPIRED_TIME = UserConfig.PTR * 2;

    public static final Cache<Integer, Boolean> ERRLIST = CacheBuilder.newBuilder()
            .expireAfterWrite(EXPIRED_TIME, TimeUnit.MILLISECONDS)
            .expireAfterAccess(EXPIRED_TIME, TimeUnit.MILLISECONDS)
            .maximumSize(10000)
            .build();

    public static void addErrorNode(Integer errid) {
        ERRLIST.put(errid, Boolean.TRUE);
    }

    public static int[] getErrorIds() {
        List<Integer> ids = new ArrayList(ERRLIST.asMap().keySet());
        int[] idlist = new int[ids.size() > 300 ? 300 : ids.size()];
        int count = 0;
        for (Integer id : ids) {
            idlist[count] = id;
            count++;
            if (count >= idlist.length) {
                break;
            }
        }
        return idlist;
    }
}
