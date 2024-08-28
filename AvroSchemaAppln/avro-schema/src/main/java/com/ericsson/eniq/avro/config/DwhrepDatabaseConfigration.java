/*
 * package com.ericsson.eniq.avro.config;
 * 
 * import javax.persistence.EntityManagerFactory; import javax.sql.DataSource;
 * 
 * import org.springframework.beans.factory.annotation.Qualifier; import
 * org.springframework.boot.context.properties.ConfigurationProperties; import
 * org.springframework.boot.jdbc.DataSourceBuilder; import
 * org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder; import
 * org.springframework.context.annotation.Bean; import
 * org.springframework.context.annotation.Configuration; import
 * org.springframework.context.annotation.Primary; import
 * org.springframework.data.jpa.repository.config.EnableJpaRepositories; import
 * org.springframework.orm.jpa.JpaTransactionManager; import
 * org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean; import
 * org.springframework.transaction.PlatformTransactionManager; import
 * org.springframework.transaction.annotation.EnableTransactionManagement;
 * 
 * @Configuration
 * 
 * @EnableTransactionManagement
 * 
 * @EnableJpaRepositories(entityManagerFactoryRef =
 * "dwhrepEntityManagerFactory", transactionManagerRef =
 * "dwhrepTransactionManager", basePackages = {
 * "com.ericsson.oss.eniq.dataingress.dwhrep.repository" }) public class
 * DwhrepDatabaseConfigration {
 * 
 * @Bean(name = "dwhrep")
 * 
 * @ConfigurationProperties("dwhrep.datasource") public DataSource dataSource2()
 * { return DataSourceBuilder.create().build(); }
 * 
 * @Bean(name = "dwhrepEntityManagerFactory")
 * 
 * @Primary public LocalContainerEntityManagerFactoryBean
 * dwhrepEntityManagerFactory(EntityManagerFactoryBuilder builder,
 * 
 * @Qualifier("dwhrep") DataSource dataSource) { return
 * builder.dataSource(dataSource).packages("com.ericsson.oss.eniq.dataingress").
 * persistenceUnit("dwhrep") .build(); }
 * 
 * @Bean(name = "dwhrepTransactionManager") public PlatformTransactionManager
 * barTransactionManager(
 * 
 * @Qualifier("dwhrepEntityManagerFactory") EntityManagerFactory
 * dwhrepEntityManagerFactory) { return new
 * JpaTransactionManager(dwhrepEntityManagerFactory); }
 * 
 * }
 */