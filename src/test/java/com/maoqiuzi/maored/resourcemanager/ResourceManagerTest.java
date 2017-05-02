package com.maoqiuzi.maored.resourcemanager;

import com.maoqiuzi.mapred.resourcemanager.ResourceManager;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by wangjinpeng on 4/21/15.
 */
public class ResourceManagerTest {

    @Test
    public void testGetOSMemory() throws Exception {
        ResourceManager manager = new ResourceManager();
        System.out.println(manager.getOSMemory());
    }
}