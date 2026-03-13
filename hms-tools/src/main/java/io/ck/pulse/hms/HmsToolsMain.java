/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.hms;

import java.lang.reflect.Method;
import java.net.URL;

public class HmsToolsMain {

    public static void main(String[] args) throws Exception {
        configureLogging();
        Class<?> mainClass = Class.forName("io.ck.pulse.hms.HmsLatencyProbe");
        Method mainMethod = mainClass.getMethod("main", String[].class);
        mainMethod.invoke(null, new Object[] {args});
    }

    private static void configureLogging() {
        if (System.getProperty("log4j.configurationFile") != null || System.getProperty("log4j2.configurationFile") != null) {
            return;
        }
        URL config = HmsToolsMain.class.getClassLoader().getResource("log4j2.xml");
        if (config != null) {
            String location = config.toString();
            System.setProperty("log4j.configurationFile", location);
            System.setProperty("log4j2.configurationFile", location);
        }
        System.setProperty("log4j2.disableJmx", "true");
        System.setProperty("log4j2.disable.jmx", "true");
        System.setProperty("log4j2.statusLoggerLevel", "OFF");
        System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "OFF");
    }
}
