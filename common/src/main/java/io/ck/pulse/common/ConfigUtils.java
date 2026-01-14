/*
 * Copyright (c) 2025 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigUtils {

    public static Properties loadConfig() {
        String currentDir = System.getProperty("user.dir");
        System.out.println(currentDir);
         
        // file path is relative to the current directory
        String configFile = currentDir + "/config.properties";
        if(!new File(configFile).exists()) {
            System.out.println("Config file not found: " + configFile);
            System.out.println("Please make sure the config.properties file is in the current directory");
            System.exit(1);
        }
        // read config file
        Properties properties = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream("config.properties")) {
            properties.load(fileInputStream);
        } catch (IOException e) {
            System.out.println("Error loading config file: " + e.getMessage());
            e.printStackTrace();

        }
        return properties;
    }
}
