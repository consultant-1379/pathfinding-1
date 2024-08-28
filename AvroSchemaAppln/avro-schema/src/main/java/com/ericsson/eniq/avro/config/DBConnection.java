package com.ericsson.eniq.avro.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

@Component
public class DBConnection {
	@Value("${spring.datasource.driverClassName}")
	private String DRIVER;
	@Value("${spring.datasource.url}")
	private String JDBC_URL;
	@Value("${spring.datasource.username}")
	private String USERNAME;
	@Value("${spring.datasource.password}")
	private String PASSWORD;

	public DataSource getDataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(this.DRIVER);
		dataSource.setUrl(this.JDBC_URL);
		dataSource.setUsername(this.USERNAME);
		dataSource.setPassword(this.PASSWORD);
		return dataSource;
	}
}
