package com.eco.data.hadoop.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * @auther lucifer
 * @mail wangxiyue.xy@163.com
 * @date 2021-03-03 10:41
 * @projectName hadoop-web
 * @description:
 */
@Service
public class HadoopService {

    @Autowired
    Connection hadoopConnection;

    @Value("${hadoop.hbase.table:StationDB}")
    private String TABLENAME;


    /*
     * @author lucifer 2021-03-03 15:00
     * @description:
     * <p>功能: rowKey模糊查询</p>
     */
    public List<HashMap<String,String>> find(String rowKey, String col){
        try {
            Table table = hadoopConnection.getTable(TableName.valueOf(TABLENAME));
            Get get  = new Get(rowKey.getBytes());
            FilterList filterList = new FilterList();
            PrefixFilter prefixFilter = new PrefixFilter(rowKey.getBytes());
            filterList.addFilter(prefixFilter);
//            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKey));


//            ColumnPrefixFilter colFilter  =new ColumnPrefixFilter(col.getBytes());
//            filterList.addFilter(colFilter);
            Scan s = new Scan();
            String colFamilay = "stationinfo";
            s.withStartRow(rowKey.getBytes());
            s.addFamily(colFamilay.getBytes());
            s.setFilter(filterList);
            ResultScanner scanner = table.getScanner(s);

            List<HashMap<String,String>> res  =new ArrayList<>();
            for (Result result : scanner) {
                HashMap<String,String> c  = new HashMap<>();
                for (Cell cell : result.listCells()) {
                   c.put(new String(CellUtil.cloneRow(cell)) +"==>"+ new String(CellUtil.cloneQualifier(cell)),new String(CellUtil.cloneValue(cell)));
                }
                res.add(c);
            }
            return res;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String find(String rowKey,String colFamily,String col){
        try {
            Table table = hadoopConnection.getTable(TableName.valueOf(TABLENAME));
            Get get  = new Get(rowKey.getBytes());
            get.addColumn(colFamily.getBytes(),col.getBytes());
            Result result = table.get(get);
            String val = new String (result.getValue(colFamily.getBytes(),col.getBytes()));
            return val;
        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;
    }
}
