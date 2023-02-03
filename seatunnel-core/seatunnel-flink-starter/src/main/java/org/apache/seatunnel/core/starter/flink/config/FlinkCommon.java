/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flink.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.common.utils.ReflectionUtils;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.function.BiConsumer;

@Slf4j
public class FlinkCommon {

    /**
     * Add jar url to classloader. The different engine should have different logic to add url into
     * their own classloader
     */
    public static BiConsumer<ClassLoader, URL> ADD_URL_TO_CLASSLOADER = (classLoader, url) -> {
        log.info("load jar -> 【{}】, use classloader -> 【{}】", url, classLoader);
        if (classLoader.getClass().getName().endsWith("SafetyNetWrapperClassLoader")) {
            URLClassLoader c = (URLClassLoader) ReflectionUtils.getField(classLoader, "inner").get();
            ReflectionUtils.invoke(c, "addURL", url);
        } else if (classLoader instanceof URLClassLoader) {
            ReflectionUtils.invoke(classLoader, "addURL", url);
        } else {
            throw new RuntimeException("Unsupported classloader: " + classLoader.getClass().getName());
        }
    };

}
