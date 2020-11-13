package com.hdsx.osp.ais.util;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 数据库操作相应配置类
 */
@Component
@ConfigurationProperties(prefix = "task")
@Data
public class DBConfig {
    private String cron;
    private Integer status;
    private String filepath;
    private String ip;
    private String dbname;
    private String username;
    private String password;

}
