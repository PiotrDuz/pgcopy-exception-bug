package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


public class DataSourceFactory {


        public static HikariDataSource create(String url, String username, String password) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl( url );
            config.setUsername( username );
            config.setPassword( password );
            config.addDataSourceProperty("maximumPoolSize", "1");
            return new HikariDataSource( config );
        }

}
