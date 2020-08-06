package com.yiqi.HBase.util;

import com.google.common.io.Resources;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @description: HBase工具类
 * @author: Wang Hongyu
 * @date: 2020-08-06
 */
@Getter
public class HBaseUtil implements IHBaseUtil {
    private String zk = null;
    private Configuration configuration = null;
    private Connection connection = null;
    private Admin admin = null;
    private Table table = null;

    /**
     * 构造方法
     * @param zk zookeeper host:post
     */
    public HBaseUtil(String zk){
            this.zk = zk;
            this.configuration = new Configuration();
            this.configuration.set("hbase.zookeeper.quorum",this.zk);
    }
    /**
     * 构造方法
     */
    public HBaseUtil(){
        this.configuration = HBaseConfiguration.create();
        this.configuration.addResource(Resources.getResource("hbase-site.xml"));
    }

    /**
     * 初始化 Connection
     * @return HBaseUtil
     */
    public HBaseUtil initConnection(){
        try{
            this.connection = ConnectionFactory.createConnection(configuration);
        }catch (IOException e){
            e.printStackTrace();
        }
        return this;
    }

    /**
     * 初始化 Admin
     * @return
     */
    public HBaseUtil initAdmin(){
        try {
            this.admin = this.connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
        return this;
    }

    /**
     * 初始化 Table
     * @param tableName
     * @return
     */
    public HBaseUtil initTable(String tableName){
        try {
            this.table = this.connection.getTable(TableName.valueOf(tableName));
        }catch (IOException e){
            e.printStackTrace();
        }
        return this;
    }


    @Override
    public void createTable(String tableName, String familyName){
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        hTableDescriptor.addFamily(new HColumnDescriptor(familyName));
        createTable(hTableDescriptor);
    }

    @Override
    public void createTable(HTableDescriptor hTableDescriptor){
        try {
            admin.createTable(hTableDescriptor);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void createTable(HTableDescriptor hTableDescriptor, @NonNull byte[][] splitKeys){
        try {
            admin.createTable(hTableDescriptor,splitKeys);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void createTable(HTableDescriptor hTableDescriptor, byte[] startKey, byte[] endKey, int numRegions){
        try {
            admin.createTable(hTableDescriptor,startKey,endKey,numRegions);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void createTableAsync(HTableDescriptor hTableDescriptor, byte[][] splitKeys){
        try {
            admin.createTableAsync(hTableDescriptor,splitKeys);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void put(String rowKey, String family, String qualifier, String value){
        try {
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(),qualifier.getBytes(),value.getBytes());
            table.put(put);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void put(Put put){
        try {
            table.put(put);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void put(List<Put> puts){
        try {
            table.put(puts);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> get(String row, String family1, List<String> qualifiers1, String family2, List<String> qualifiers2){
        Map<String,Object> resultMap = new HashMap<>();
        Get get = new Get(row.getBytes());
        qualifiers1.forEach(qualifier -> get.addColumn(family1.getBytes(),qualifier.getBytes()));
        qualifiers2.forEach(qualifier -> get.addColumn(family2.getBytes(),qualifier.getBytes()));
        try {
            Result rs = table.get(get);
            qualifiers1.forEach(qualifier ->{
                Cell cell = rs.getColumnLatestCell(family1.getBytes(),qualifier.getBytes());
                resultMap.put(qualifier,cell != null ? Bytes.toString(CellUtil.cloneValue(cell)) :null);
            });
            qualifiers1.forEach(qualifier ->{
                Cell cell = rs.getColumnLatestCell(family2.getBytes(),qualifier.getBytes());
                resultMap.put(qualifier,cell != null ? Bytes.toString(CellUtil.cloneValue(cell)) :null);
            });
        } catch (IOException e){
            e.printStackTrace();
        }
        return resultMap;
    }

    @Override
    public Map<String, Object> get(String row, String family, List<String> qualifiers){
        Get get = new Get(row.getBytes());
        Map<String,Object> resultMap = new HashMap<>();
        qualifiers.forEach(qualifier->get.addColumn(family.getBytes(),qualifier.getBytes()));
        try {
            Result rs = table.get(get);
            qualifiers.forEach(qualifier->{
                Cell cell = rs.getColumnLatestCell(family.getBytes(),qualifier.getBytes());
                resultMap.put(qualifier,cell != null ? Bytes.toString(CellUtil.cloneValue(cell)) :null);
            });
        } catch (IOException e){
            e.printStackTrace();
        }
        return resultMap;
    }

    @Override
    public void addColumn(String tableName, String family) {
        try{
            admin.addColumn(TableName.valueOf(tableName),new HColumnDescriptor(family));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void deleteTable(String tableName) {
        try {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void releaseResource() {
        if(table != null){
            try {
                table.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        if (admin != null){
            try {
                admin.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        if(connection != null){
            try {
                connection.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
