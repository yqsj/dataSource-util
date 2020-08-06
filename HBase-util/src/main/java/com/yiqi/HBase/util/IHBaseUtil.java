package com.yiqi.HBase.util;

import lombok.NonNull;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @description: 工具类接口
 * @author: Wang Hongyu
 * @date: 2020-08-06
 */
public interface IHBaseUtil {
    /**
     * 创建一个新表
     *
     * @param tableName  表名称
     * @param familyName 列族名称
     */
    public void createTable(String tableName, String familyName);

    /**
     * 创建一个新表
     *
     * @param hTableDescriptor
     */
    public void createTable(HTableDescriptor hTableDescriptor);

    /**
     * 使用指定的分割键定义的初始空区域创建一个新表。其中创建的区域总数总是将分割的数目加1。PS.避免传递空分割键
     *
     * @param hTableDescriptor HBase Table描述类
     * @param splitKeys        表初始区域的分割数组
     * @throws IOException
     */
    public void createTable(HTableDescriptor hTableDescriptor, @NonNull byte[][] splitKeys);

    /**
     * 创建一个包含指定区域数的新表。
     * 第一个区域的结束键为指定的开始键，开始表中最后一个区域的键(第一个区域的起始键为空，最后一个区域的起始键和结束键都为空)。
     *
     * @param hTableDescriptor HBase Table描述类
     * @param startKey 键开始范围
     * @param endKey 键结束范围
     * @param numRegions 要创建的region总数
     * @throws IOException
     */
    public void createTable(HTableDescriptor hTableDescriptor, @NonNull byte[] startKey, byte[] endKey, int numRegions);

    /**
     * 异步创建一个新表
     * @param hTableDescriptor HBase Table描述类
     * @param splitKeys 表初始区域的分割数组
     * @throws IOException
     */
    public void createTableAsync(HTableDescriptor hTableDescriptor, @NonNull byte[][] splitKeys);

    /**
     * put 操作
     * @param rowKey 行键
     * @param family 列族
     * @param qualifier 列键
     * @param value value值
     * @throws IOException
     */
    public void put(String rowKey,String family,String qualifier,String value);

    /**
     * put 操作
     * @param put 执行put的数据
     * @throws IOException
     */
    public void put(Put put);

    /**
     * 将数据批量放入表中。
     * @param puts put数据列表
     * @throws IOException
     */
    public void put(List<Put> puts);

    /**
     * get 操作
     * @param row 行键
     * @param family1 列族
     * @param qualifiers1 列键
     * @param family2 列族
     * @param qualifiers2 列键
     * @return Map
     * @throws IOException
     */
    public Map<String,Object> get(String row,String family1,List<String> qualifiers1,String family2,
                                  List<String> qualifiers2);

    /**
     * get 操作
     * @param row 行键
     * @param family 列族
     * @param qualifiers 列键
     * @return Map
     * @throws IOException
     */
    public Map<String,Object> get(String row,String family,List<String> qualifiers);

    /**
     * 向已存在的表中添加列族
     * @param tableName 表名称
     * @param family 列族
     */
    public void addColumn(String tableName,String family);

    /**
     * 删除表
     * @param tableName 表名称
     */
    public void deleteTable(String tableName);

    /**
     * 资源释放
     */
    public void releaseResource();

}
