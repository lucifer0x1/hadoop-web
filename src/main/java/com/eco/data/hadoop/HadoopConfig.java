package com.eco.data.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @auther lucifer
 * @mail wangxiyue.xy@163.com
 * @date 2021-03-03 10:44
 * @projectName hadoop-web
 * @description:
 */
@Component
public class HadoopConfig {

    private static Admin hadoopAdmin;

    @Value("${hadoop.zookeeper.host:10.198.192.76}")
    private String IP;

    public HadoopConfig(){

    }

    @Bean
    public Configuration hadoopConfiguration(){
        Configuration hadoopConfiguration = null;
        try {
            hadoopConfiguration = HBaseConfiguration.create();

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            hadoopConfiguration.set("hbase.zookeeper.quorum", IP);
            System.out.println(IP);
        }
        return hadoopConfiguration;
    }


    @Bean
    public Connection hadoopConnection(Configuration conf){
        Connection hadoopConnection = null;
        try {
            hadoopConnection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hadoopConnection;
    }

    @Bean
    public Admin hadoopAdmin(Connection connection){
        Admin hadoopAdmin = null;
        try {
            hadoopAdmin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hadoopAdmin;
    }

}
