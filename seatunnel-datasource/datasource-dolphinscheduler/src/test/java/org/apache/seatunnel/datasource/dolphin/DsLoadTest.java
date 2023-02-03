package org.apache.seatunnel.datasource.dolphin;

import org.apache.seatunnel.apis.base.api.BaseDataSource;

import java.util.ServiceLoader;

public class DsLoadTest {

    public static void main(String[] args) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader<BaseDataSource> serviceLoader = ServiceLoader.load(BaseDataSource.class, classLoader);
        try {
            Class<?> aClass = Class.forName("org.apache.seatunnel.datasource.dolphin.DolphinDataSource");
            Object o = aClass.newInstance();
            System.out.println(o);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        System.out.println(111);
    }
}
