package com.maoqiuzi.maored.common;

import com.maoqiuzi.mapred.common.Hdfs;
import org.junit.Test;

/**
 * Created by wangjinpeng on 4/20/15.
 */
public class HdfsTest {

    @Test
    public void testGetFileSize() throws Exception {
        Hdfs hdfs = new Hdfs();
        Long size = hdfs.getFileSizeS3("input/medianinput");
        System.out.println(size);
    }
}