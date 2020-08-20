package com.yiqi.HBase.test;


import com.yiqi.HBase.util.ByteUtils;
import com.yiqi.HBase.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

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
        hBaseUtil.initConnection().initAdmin().initTable("Data:Imsi");
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
//            NavigableMap<String,Object> resultMap = new TreeMap<>();
//            ResultScanner resultScanner = table.getScanner(scan);
//            resultScanner.forEach(result -> System.out.println(Bytes.toString(result.getValue("info1".getBytes(),"name".getBytes()))));
//
            Get get = new Get("row101".getBytes());
            Put put = new Put("row101".getBytes());
            List<Row> rows = new ArrayList<>();
            get.addFamily("info1".getBytes());
            put.addColumn("info1".getBytes(),"name".getBytes(),"王宇".getBytes());
            rows.add(get);
            rows.add(put);
            Result[] results = hBaseUtil.batchOption(rows);
            Arrays.stream(results).forEach(result->{
                Cell cell = result.getColumnLatestCell("info1".getBytes(),"name".getBytes());
                System.out.println( cell != null ? Bytes.toString(CellUtil.cloneValue(cell)) : null);
            });
        }catch (Exception e){
            e.printStackTrace();
        }

        hBaseUtil.releaseResource();
    }

    @Test
    public void test(){
        table = hBaseUtil.getTable();
        Scan scan = new Scan();
        scan.setLimit(5);
        try {
            ResultScanner resultScanner = table.getScanner(scan);
            List<byte[]> rowkeys = new ArrayList<>();
            resultScanner.forEach(result -> {
                try {
                    rowkeys.add(Bytes.toBytes(ByteUtils.uncompress(CellUtil.cloneValue(result.getColumnLatestCell("c".getBytes(), "c".getBytes())))));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            rowkeys.stream().forEach(i-> System.out.println(Bytes.toString(i)));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void test2(){
        byte[] _bytes = "a".getBytes();
        for (int i = 0; i < _bytes.length; i++) {
            System.out.println(_bytes[i]);
            _bytes[i] = (byte) (_bytes[i] + 1);
            System.out.println(_bytes[i]);
        }

    }


}
