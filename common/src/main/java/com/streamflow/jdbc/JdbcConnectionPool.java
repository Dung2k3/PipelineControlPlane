package com.streamflow.jdbc;

import com.streamflow.controlplane.config.AppConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * 1 HikariDataSource dung chung cho moi node JDBC_ENRICH trong JVM (khong phai 1 pool rieng moi
 * node - ton connection vo ich), thread-safe cho nhieu StreamThread cua Kafka Streams goi dong thoi.
 * Lazy static init - pipeline nao khong dung node JDBC_ENRICH thi khong bao gio dung toi class nay,
 * khong co ket noi DB vo ich. Doc cau hinh qua env var, cung convention voi BOOTSTRAP_SERVERS/
 * PIPELINE_ID da dung o OrderPaymentJoinedApp.
 */
public final class JdbcConnectionPool {

    private static final HikariDataSource DATA_SOURCE = build();

    private JdbcConnectionPool() {
    }

    public static DataSource dataSource() {
        return DATA_SOURCE;
    }

    private static HikariDataSource build() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(AppConfig.get("JDBC_URL"));
        config.setUsername(AppConfig.get("JDBC_USERNAME"));
        config.setPassword(AppConfig.get("JDBC_PASSWORD"));
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }
}
