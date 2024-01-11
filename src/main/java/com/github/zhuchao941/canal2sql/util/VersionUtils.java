package com.github.zhuchao941.canal2sql.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class VersionUtils {

    public static String getJarVersion() {
        InputStream manifestStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("META-INF/MANIFEST.MF");
        if (manifestStream == null) {
            // 尝试Spring Boot JAR的位置
            manifestStream = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("BOOT-INF/classes/META-INF/MANIFEST.MF");
        }
        Properties properties = new Properties();
        try {
            properties.load(manifestStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (manifestStream != null) {
                try {
                    manifestStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return properties.getProperty("Implementation-Version");
    }
}
