package org.apache.seatunnel.datasource.dolphin;

import org.apache.seatunnel.api.datasource.BaseDataSource;

import java.util.ServiceLoader;

public class DsLoadTest {

    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader<BaseDataSource> serviceLoader = ServiceLoader.load(BaseDataSource.class, classLoader);
        Class<?> aClass = Class.forName("org.apache.seatunnel.datasource.dolphin.DolphinDataSource");
        Object o = aClass.newInstance();
        System.out.println(o.getClass().getName());

    }
}
