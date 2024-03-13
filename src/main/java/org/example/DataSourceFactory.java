package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;


public class DataSourceFactory {
    static {
        InputStream resourceAsStream = Main.class.getResourceAsStream("/logging.properties");
        try {
            LogManager.getLogManager().readConfiguration(resourceAsStream);
        } catch (final IOException e) {
            Logger.getAnonymousLogger().severe("Could not load default logging.properties file");
            Logger.getAnonymousLogger().severe(e.getMessage());
        }
    }

    public static HikariDataSource create(String url, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.addDataSourceProperty("maximumPoolSize", "1");
        return new HikariDataSource(config);
    }

}
