package com.yiqi.HBase.test;


import com.yiqi.HBase.util.HBaseUtil;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * @description: HBase工具类测试
 * @author: Wang Hongyu
 * @date: 2020-08-06
 */
public class TestHBaseUtil {
    private static HBaseUtil hBaseUtil;
    private static Table table;
    static {
        hBaseUtil = new HBaseUtil();
        hBaseUtil.initConnection().initAdmin().initTable("why_test");
        table = hBaseUtil.getTable();
    }

    public static void main(String[] args) {
//        hBaseUtil.createTable("why_test2","name");
////        hBaseUtil.put("row99","name","name","99");
//        List<Put> puts = new ArrayList<>();
//        for(int i =0;i<100;i++){
//            Put put = new Put(("row"+i).getBytes());
//            put.addColumn("info1".getBytes(),"name".getBytes(),("33"+i).getBytes());
//            puts.add(put);
//        }
//        hBaseUtil.put(puts);
//        List<String> qualifiers = new ArrayList<>();
//        qualifiers.add("c");
//        qualifiers.add("name");
//        Map<String,Object> resultMap = hBaseUtil.get("row99","info1",qualifiers);
//        Object result = resultMap.get("name");
//        Map<String,Object> resultMap = hBaseUtil.get("row1","info1",qualifiers,"info1",qualifiers);
        Scan scan = new Scan();

        scan.addFamily("info1".getBytes());
        scan.setBatch(10);
//        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator("row99".getBytes()));
//        scan.setFilter(rowFilter);
        Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator("334".getBytes()));
        scan.setFilter(valueFilter);
        try{
//            NavigableMap<String,Object> navigableMap = new TreeMap<>();
//            ResultScanner resultScanner = table.getScanner(scan);
//            resultScanner.forEach(result -> {
//                System.out.println(Bytes.toString(result.getValue("info1".getBytes(),"name".getBytes())));
//            });
            NavigableMap<String,Object> resultMap = new TreeMap<>();
            ResultScanner resultScanner = table.getScanner(scan);
            resultScanner.forEach(result -> System.out.println(Bytes.toString(result.getValue("info1".getBytes(),"name".getBytes()))));
        }catch (Exception e){
            e.printStackTrace();
        }

        hBaseUtil.releaseResource();
    }


}
