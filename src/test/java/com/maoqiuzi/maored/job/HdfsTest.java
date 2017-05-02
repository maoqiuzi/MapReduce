package com.maoqiuzi.maored.job;

import com.maoqiuzi.mapred.common.Hdfs;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by wangjinpeng on 4/12/15.
 */
public class HdfsTest {

    private Hdfs hdfs = null;

    @Before
    public void beforeEachTest() {
        hdfs = new Hdfs();
    }
    
    @Test
    public void testSetMasterIpPort() throws Exception {
        hdfs.setMasterIpPort("192.168.0.1", 5000);
        assertEquals("192.168.0.1", hdfs.getMasterIp());
        assertEquals(5000, hdfs.getMasterPort());
    }

    @Test
    public void testRemoveS3() throws Exception {
        hdfs.setMasterIpPort("192.168.0.1", 5000);
        hdfs.removeS3("masterInfo");
    }

    @Test
    public void testDownloadS3() throws Exception {
        hdfs.setMasterIpPort("192.168.0.1", 5000);
        String dest = "tempdownloadfile";
        hdfs.downloadS3("masterInfo", dest);
        File f = new File(dest);
        assertTrue(f.exists() && !f.isDirectory());
        assertTrue(f.delete());
    }

    @Test
    public void testGetFilesInDirectory() throws Exception {
        List<String> files = hdfs.getFilesInDirectory("input");
        for (String file : files) {
            System.out.println(file);
        }
    }
}