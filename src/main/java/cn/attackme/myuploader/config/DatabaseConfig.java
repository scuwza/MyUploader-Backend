package cn.attackme.myuploader.config;

import lombok.Data;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class DatabaseConfig {
    public static String jdbcUrl;
    public static String jdbcUser;
    public static String jdbcPassword;

    @Value("${spring.datasource.url}")
    public void setJdbcUrl(String jdbcUrl) {
        DatabaseConfig.jdbcUrl = jdbcUrl;
    }

    @Value("${spring.datasource.username}")
    public void setJdbcUser(String jdbcUser) {
        DatabaseConfig.jdbcUser = jdbcUser;
    }

    @Value("${spring.datasource.password}")
    public void setJdbcPassword(String jdbcPassword) {
        DatabaseConfig.jdbcPassword = jdbcPassword;
    }
}
