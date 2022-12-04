package com.twanhu.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnection {

    // 设置静态属性hbase连接
    public static Connection connection = null;

    static {
        // 创建hbase连接
        try {
            // 使用配置文件的方法
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            System.out.println("获取连接失败");
            e.printStackTrace();
        }
    }

    /**
     * 连接关闭方法，用于进程关闭时调用
     */
    public static void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

}
