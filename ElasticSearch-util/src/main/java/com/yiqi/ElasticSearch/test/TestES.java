package com.yiqi.ElasticSearch.test;

import com.yiqi.ElasticSearch.util.ElasticSearchUtil;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-24
 */
public class TestES {
    private static ElasticSearchUtil searchUtil = new ElasticSearchUtil("192.168.1.66",9200,"http");
    private static String index = "czhtest";
    private static String indexType = "czhindex1";
    private static String docId = "26";

    @Test
    public void testIndex(){
        Map<String,Object> map = new HashMap<>();
        map.put("doom","dad");
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
            GetResponse getResponse = searchUtil.execGet("czhtest","czhindex1","26");
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
        map.put("doom","lxf");
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
            DeleteResponse deleteResponse = searchUtil.execDelete(index,indexType,"6");
            System.out.println(deleteResponse);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Test
    public void testSearch1(){
        try {
            SearchResponse searchResponse = searchUtil.execSearch(index,indexType);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            Arrays.stream(searchHits).forEach(i->{
                System.out.println(i.getScore());
                Map<String,Object> map = i.getSourceAsMap();
                map.entrySet().forEach(j->{
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
            SearchResponse searchResponse = searchUtil.execSearch(index,indexType,1,99,
                    new TermsQueryBuilder("name","黑木"));
            List<Map<String,Object>> mapList = searchUtil.getMapFromSearchRes(searchResponse);
            mapList.parallelStream().forEach(i->{
              i.entrySet().parallelStream().forEach(j->{
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
            SearchResponse searchResponse = searchUtil.execSearch(index,indexType,1,99,
                    new MatchQueryBuilder("name","黑木")
                            .fuzziness(Fuzziness.AUTO)
                            .prefixLength(3)
                            .maxExpansions(10)
            );
            List<Map<String,Object>> mapList = searchUtil.getMapFromSearchRes(searchResponse);
            mapList.parallelStream().forEach(i->{
                i.entrySet().parallelStream().forEach(j->{
                    System.out.println(j.getKey());
                    System.out.println(j.getValue());
                });
            });

        }catch (IOException e){
            e.printStackTrace();
        }
    }



}
