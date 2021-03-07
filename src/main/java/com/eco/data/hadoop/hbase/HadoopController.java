package com.eco.data.hadoop.hbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.List;


/**
 * @auther lucifer
 * @mail wangxiyue.xy@163.com
 * @date 2021-03-03 10:40
 * @projectName hadoop-web
 * @description:
 */
@RestController
@RequestMapping("/")
public class HadoopController {

    @Autowired
    com.eco.data.hadoop.hbase.HadoopService service;

    @RequestMapping("/get")
    public String getOne(String rowKey,String colFamily,String col){
        Date st = new Date();
        String v = service.find(rowKey,colFamily,col);
        Date ed = new Date();
        System.out.println("rowKey=" +rowKey+ " , "  + colFamily + ":" + col+  " ===>" +  (ed.getTime()-st.getTime()));
       return v;
    }

    @RequestMapping("/s")
    public List select(String rowKey,String col){
        List res = null;
        Date st = new Date();
        res = service.find(rowKey,col);
        Date ed = new Date();
        System.out.println("rowKey=" +rowKey+ " , " + col+  " ===>" +  (ed.getTime()-st.getTime()));
        return res;
    }
}
