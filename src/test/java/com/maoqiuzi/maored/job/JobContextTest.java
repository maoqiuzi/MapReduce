package com.maoqiuzi.maored.job;

import org.junit.Test;

import java.util.Calendar;

import static org.junit.Assert.*;

/**
 * Created by wangjinpeng on 4/21/15.
 */
public class JobContextTest {

    @Test
    public void testGetJobId() throws Exception {
        long jobid = System.currentTimeMillis();
        String s = Long.valueOf(jobid).toString();
        System.out.print(s);
    }
}