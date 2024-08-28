package com.ericsson.eniq.data.catalog.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class WebConfig {

    @Bean(name="dwhdb")
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource dwhDB(){
        return DataSourceBuilder.create().build();
    }

    @Bean(name="dwhDBTemplate")
    public JdbcTemplate dwhDBTemplate(@Qualifier("dwhdb") DataSource ds){
        return new JdbcTemplate(ds);
    }

    @Bean(name="etlrepdb")
    @ConfigurationProperties(prefix = "spring.second-db")
    public DataSource etlrepDB(){
        return DataSourceBuilder.create().build();
    }

    @Bean(name="etlrepDBTemplate")
    public JdbcTemplate etlrepDBTemplate(@Qualifier("etlrepdb") DataSource ds){
        return new JdbcTemplate(ds);
    }


}
