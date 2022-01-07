package com.elevenquest.athena.jdbctest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource; 

public class DataSourceConfiguration {
  
  public static HikariDataSource getDataSource() {
    HikariConfig config = new HikariConfig();
    // TODO : Change 'S3OutputLocation' parameter on JDBC string.
    config.setJdbcUrl("jdbc:awsathena://AwsRegion=ap-northeast-2;S3OutputLocation=s3://xxxxxxx/output");
    config.setDriverClassName("com.simba.athena.jdbc42.Driver");
    config.addDataSourceProperty("AwsCredentialsProviderClass", "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain");
    config.setConnectionTimeout(300000);
    config.setIdleTimeout(10000);
    config.setMinimumIdle(0);
    config.setMaximumPoolSize(15);
    config.setPoolName("athenads");
    config.setRegisterMbeans(true);
    HikariDataSource ds = new HikariDataSource(config);
    return ds;
  }
}
