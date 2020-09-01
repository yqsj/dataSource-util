package com.yiqi.ElasticSearch.test;

import com.yiqi.ElasticSearch.util.ESRestHighClientUtil;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.junit.Test;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-24
 */
public class TestES {
    private static ESRestHighClientUtil searchUtil = new ESRestHighClientUtil("localhost",9200,"http");
    private static String index = "why_test";
    private static String indexType = "type1";
    private static String docId = "41";

    @Test
    public void testIndex(){
        Map<String,Object> map = new HashMap<>();
        map.put("name","这是一段很随意的话，请不要在意。");
        try{
            IndexResponse indexResponse = searchUtil.execIndex(index,indexType,docId,map);
            System.out.println(indexResponse);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Test
    public void testGet(){
        try {
            GetResponse getResponse = searchUtil.execGet(index,indexType,docId);
            Map<String,Object> map = getResponse.getSourceAsMap();
            map.entrySet().forEach(j->{
                System.out.println(j.getKey());
                System.out.println(j.getValue());
            });
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Test
    public void testExit(){
        try {
            boolean flag = searchUtil.ExitDoc(index,indexType,docId);
            System.out.println(flag);
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    @Test
    public void testUpdate(){
        Map<String,Object> map = new HashMap<>();
        map.put("uesr","lxf");
        try {
            UpdateResponse updateResponse = searchUtil.execUpdate(index,indexType,docId,true,map);
            System.out.println(updateResponse);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Test
    public void testDel(){
        try {
            DeleteResponse deleteResponse = searchUtil.execDelete(index,indexType,docId);
            System.out.println(deleteResponse);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Test
    public void testSearch1(){
        try {
            SearchResponse searchResponse = searchUtil.execSearch(index,indexType);
            List<Map<String,Object>> mapList = searchUtil.getMapFromSearchRes(searchResponse);
            mapList.forEach(i->{
                i.entrySet().forEach(j->{
                    System.out.println(j.getKey());
                    System.out.println(j.getValue());
                });
            });
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Test
    public void testSearch2(){
        try {
            SearchResponse searchResponse = searchUtil.execSearch(index,indexType,2,1,
                    new TermsQueryBuilder("name","lxf","why"));
            List<Map<String,Object>> mapList = searchUtil.getMapFromSearchRes(searchResponse);
            mapList.forEach(i->{
              i.entrySet().forEach(j->{
                  System.out.println(j.getKey());
                  System.out.println(j.getValue());
              });
            });
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void testSearch3(){
        try {
            SearchResponse searchResponse = searchUtil.execSearch(index,indexType,0,1,
                    new MatchQueryBuilder("name","随意")
                            .fuzziness(Fuzziness.AUTO)
                            .prefixLength(2) //不会被"模糊化"的初始字符数量。这有助于减少必须检查的术语数量。默认为0
                            .maxExpansions(10) //模糊查询将扩展到的最大条数。默认为50
            );
            List<Map<String,Object>> mapList = searchUtil.getMapFromSearchRes(searchResponse);
            mapList.forEach(i->{
                i.entrySet().forEach(j->{
                    System.out.println(j.getKey());
                    System.out.println(j.getValue());
                });
            });

        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
