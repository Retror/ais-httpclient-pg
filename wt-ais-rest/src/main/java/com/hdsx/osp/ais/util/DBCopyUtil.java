package com.hdsx.osp.ais.util;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Component
@Slf4j
public class DBCopyUtil {

    private static CopyManager copyManager = null;
    private static Connection conn = null;
    private static DBConfig dbConfig;

    public static String ip;
    public static String dbname;
    public static String username;
    public static String password;
    public static String filepath;
    @Autowired
    private DBConfig config;

    /**
     * 静态方法想使要使用一个非静态对象，需要做一个初始化【重要】
     */
    @PostConstruct
    public void init() {
        dbConfig = config;
        ip = config.getIp();
        dbname = config.getDbname();
        username = config.getUsername();
        password = config.getPassword();
        filepath = config.getFilepath();
    }

    /**
     * 获取copymanager对象
     *
     * @return
     */
    public CopyManager getCopyManager() {
        CopyManager copyManager = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://" + ip + ":5432/" + dbname, username, password);
            copyManager = new CopyManager((BaseConnection) conn);
            log.info("获取copymanager对象成功");
        } catch (Exception ex) {
            log.info("获取copymanager对象失败，原因:{}", ex.getMessage());
        }
        return copyManager;
    }

    //导入数据到数据表当中
    public static String restoreJob(String fileName, String tableName) {
        log.info("使用copy指令将数据还原至{}临时表当中...", tableName);
        long begin = System.currentTimeMillis();
        String status = "success";
        FileInputStream fileInputStream = null;
        //String tableName = "ais_base_temp";
        try {
            copyManager = new DBCopyUtil().getCopyManager();
            fileInputStream = new FileInputStream(fileName);
            copyManager.copyIn("COPY " + tableName + " FROM STDIN DELIMITER AS '\t' csv header", fileInputStream);
        } catch (FileNotFoundException e) {
            status = "fail";
            log.info("文件未找到，错误原因:{}", e.getMessage());
        } catch (SQLException e) {
            status = "fail";
            log.info("sql执行异常,原因:{}", e.getMessage());
        } catch (IOException e) {
            status = "fail";
            log.info("io写入异常,原因:{}", e.getMessage());
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    status = "fail";
                    log.info("文件输出流未正常关闭,原因:{}", e.getMessage());
                }
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                status = "fail";
                log.info("数据库连接未正常关闭，原因:{}", e.getMessage());
            }
        }
        long end = System.currentTimeMillis();
        log.info(tableName + "临时表数据还原完成，用时：{}", end - begin);
        return status;
    }
}
