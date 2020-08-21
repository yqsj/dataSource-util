package com.yiqi.ElasticSearch.test;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-04
 */
public class TestESJavaAPI {
     private static RestHighLevelClient client;
     static {
         client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.1.66",9200,"http")));
     }

    public static void main(String[] args) {
        testIndexRequestAPI();
        testGetRequestAPI();
        testExistsRequestAPI();
        testDeleteRequestAPI();
        testUpdateRequestAPI();
        testBulkRequestAPI();
        testSearchRequestAPI();
        try {
            client.close();
        }catch (IOException e){
            e.printStackTrace();
        }
     }

//        "why_test","type1","4"

    public static void testIndexRequestAPI(){
        IndexRequest request;
        request = new IndexRequest("why_test","type1","4");
        // 这里有多种形式，这里只使用了Map，还有JSON、XContentBuilder对象
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("uesr","kimhy");
        jsonMap.put("postDate",new Date());
        jsonMap.put("postate",new Date());
        jsonMap.put("message","tch");
        jsonMap.put("name","trave0lying out Elasticsearch");
        request.source(jsonMap);
        try {
            //同步执行
            IndexResponse indexResponseSync = client.index(request,RequestOptions.DEFAULT);
//            System.out.println(indexResponseSync);
            //异步执行 TODO
//            client.indexAsync(request,RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
//                @Override
//                public void onResponse(IndexResponse indexResponse) {
//                    System.out.println(indexResponse);
//                }
//
//                @Override
//                public void onFailure(Exception e) {
//                    System.out.println(e);
//                }
//            });


        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void testGetRequestAPI(){
        GetRequest getRequest = new GetRequest("czhtest","czhindex1","1").version(2);
        // 禁用源检索,默认情况下启用
//        getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
//        // 为特定字段配置源
//        String[] includes = new String[]{"name","user"};
//        // 为特定字段源排除
//        String[] excludes = Strings.EMPTY_ARRAY;
//        FetchSourceContext fetchSourceContext = new FetchSourceContext(true,includes,excludes);
//        getRequest.fetchSourceContext(fetchSourceContext);
//        // 路由值
//        getRequest.parent("router");
//        //parent值
//        getRequest.parent("parent");
//        //偏好值
//        getRequest.preference("preference");
//        // 将realtime标志设置为false(默认为true)
//        getRequest.realtime(false);
//        //在检索文档之前执行刷新(默认为false)
//        getRequest.refresh(true);
//        //版本
//        getRequest.version(3);
//        //版本类型
//        getRequest.versionType(VersionType.EXTERNAL);
        try {
            // 配置message字段的检索(要求字段分别存储在映射中)
//            getRequest.storedFields("name");
//            GetResponse getResponse = client.get(getRequest,RequestOptions.DEFAULT);
//            String message = getResponse.getField("name").getValue();

            // 同步执行 客户端在继续执行代码之前返回GetResponse
            GetResponse getResponseSync = client.get(getRequest,RequestOptions.DEFAULT);
            if(getResponseSync.isExists()){
                long version = getResponseSync.getVersion();
                // 以字符串形式检索文档
                String sourceAsString = getResponseSync.getSourceAsString();
                // 将文档检索为Map<String,Object>
                Map<String,Object> sourceAsMap = getResponseSync.getSourceAsMap();
                // 以byte[]的形式检索文档
                byte[] sourceAsBytes = getResponseSync.getSourceAsBytes();
            }else {
                /*
                 处理违章到文档的方案。其中虽然挥发的响应具有404状态代码，但返回有效的GetResponse而不是抛出异常，此类响应不包括任何原文档，并且
                 其isExits方法返回false
                 */
                System.out.println("未找到文档的方案");
            }
            // 异步执行
//            client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
//                @Override
//                public void onResponse(GetResponse documentFields) {
//                    System.out.println(documentFields);
//                }
//
//                @Override
//                public void onFailure(Exception e) {
//                    System.out.println(e);
//                }
//            });
        }catch (ElasticsearchException e){
            if(e.status() == RestStatus.NOT_FOUND){
                System.out.println("索引不存在");
            }else if(e.status() == RestStatus.CONFLICT){
                System.out.println("版本冲突");
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void testExistsRequestAPI(){
         GetRequest getRequest = new GetRequest("czhtest","czhindex1","4");
         // 禁用提取_source
         getRequest.fetchSourceContext(new FetchSourceContext(false));
         // 禁用提取存储手段
         getRequest.storedFields("_none_");
         try {
             // 同步执行
             boolean existsSync = client.exists(getRequest,RequestOptions.DEFAULT);
             // 异步执行
//             client.existsAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<Boolean>() {
//                 @Override
//                 public void onResponse(Boolean aBoolean) {
//                     System.out.println(aBoolean);
//                 }
//
//                 @Override
//                 public void onFailure(Exception e) {
//                     System.out.println(e);
//                 }
//             });
         }catch (IOException e){
             e.printStackTrace();
         }

    }

    public static void testDeleteRequestAPI(){
        DeleteRequest deleteRequest = new DeleteRequest("czhtest","czhindex1","3");
//        //路由值
//        deleteRequest.routing("routing");
//        //parent值
//        deleteRequest.routing("parent");
//        // 等待著随拍你可用的作为TimeValue的超时
//        deleteRequest.timeout(TimeValue.timeValueMinutes(2));
//        // 等待主碎片可用的作为String的超时
//        deleteRequest.timeout("2m");
//        // 将刷新策略作为WriteRequest.REfreshPolicy实例
//        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
//        // 将刷新策略作为String
//        deleteRequest.setRefreshPolicy("wait_for");
//        // 版本
//        deleteRequest.version(2);
//        // 版本类型
//        deleteRequest.versionType(VersionType.EXTERNAL);
        try {
            // 同步执行
            DeleteResponse deleteResponseSync = client.delete(deleteRequest,RequestOptions.DEFAULT);
            System.out.println(deleteResponseSync);
            // 异步
//            client.deleteAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
//                @Override
//                public void onResponse(DeleteResponse deleteResponse) {
//                    System.out.println(deleteResponse);
//                }
//
//                @Override
//                public void onFailure(Exception e) {
//                    System.out.println(e);
//                }
//            });
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void testUpdateRequestAPI(){
        UpdateRequest updateRequest = new UpdateRequest("why_test","type1","4");
        Map<String,Object> params = new HashMap<>();
        params.put("name","why");
        try {
            updateRequest.doc(params);
            //设置如果更新的文档不存在,就必须要创建一个
            updateRequest.docAsUpsert(true);
            UpdateResponse updateResponse = client.update(updateRequest,RequestOptions.DEFAULT);
            // 获取源数据
            GetResult result = updateResponse.getGetResult();
            if (result.isExists()){
                // 这里支持多种格式输出(String.byte,Map)
                Map<String,Object> sourceAsMap = result.sourceAsMap();
            }else {
                System.out.println("默认情况下不会返回文档");
            }
            // 检测是否分片失败
            ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
            if(shardInfo.getTotal() != shardInfo.getSuccessful()){
                // 成功的分片数量小于总分片数量
            }
            if(shardInfo.getFailed() > 0){
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()){
                    String reason = failure.reason();
                }
            }
            System.out.println(updateResponse);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Bulk API 批量处理
     */
    public static void testBulkRequestAPI(){
        BulkRequest request = new BulkRequest();
        // 同一个BulkRequest可以添加不同的操作类型
        request.add(new UpdateRequest("why_test","type1","4")
                .doc(XContentType.JSON,"name","6"))
                .add(new IndexRequest("why_test","type1","4")
                .source(XContentType.JSON,"name","18"));
        // 设置在批量操作前必须有几个分片处于激活状态
        request.waitForActiveShards(2);
        // 全部分片处于激活状态
//        request.waitForActiveShards(ActiveShardCount.ALL);
        // 默认
//        request.waitForActiveShards(ActiveShardCount.DEFAULT);
        // 一个
        request.waitForActiveShards(ActiveShardCount.ONE);
//        try {
//            // 同步
//            BulkResponse bulkResponses = client.bulk(request,RequestOptions.DEFAULT);
//            // 检测是否有失败
//            if(bulkResponses.hasFailures()){
//                // 至少有一个失败
//                for(BulkItemResponse bulkItemResponse : bulkResponses){
//                    // 检测给定的操作是否失败
//                    if(bulkItemResponse.isFailed()){
//                        // 获取失败的信息
//                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
//                    }
//                }
//            }
//            // 遍历所有的操作结果
//            for(BulkItemResponse bulkItemResponse : bulkResponses){
//                // 获取操作结果响应,DocWriteResponse实例可以是IndexResponse,UpdateResponse,DeleteResponse
//                DocWriteResponse docWriteResponse = bulkItemResponse.getResponse();
//                if(bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
//                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE){
//                    // index操作后的响应结果
//                    IndexResponse indexResponse = (IndexResponse) docWriteResponse;
//                }else if(bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE){
//                    // update操作后的响应结果
//                    UpdateResponse updateResponse = (UpdateResponse) docWriteResponse;
//                }else if(bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE){
//                    // delete操作后的响应结果
//                    DeleteResponse deleteResponse = (DeleteResponse) docWriteResponse;
//                }
//            }
//        }catch (IOException e){
//            e.printStackTrace();
//        }

        /*** Bulk Processor start ***/

//         //BulkProcessor.listener 对BulkRequest执行前后以及失败时监听
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                //在执行前获取操作的数量
                int numOfActions = bulkRequest.numberOfActions();
                System.out.println(numOfActions);
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                // 执行后查看响应中是否包含失败的操作
                if(bulkResponse.hasFailures()){
                    System.out.println(l);
                }else {
                    System.out.println(l);
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                // 打印失败信息
                System.out.println(throwable);
            }
        };
        // BulkProcess.Builder
        BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync,listener);
        // 指定多少操作时，就会刷新一次
        builder.setBulkActions(1000) //  达到刷新的条数
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB)) // 达到 刷新的大小
                .setFlushInterval(TimeValue.timeValueSeconds(5)) // 固定刷新的时间频率
                .setConcurrentRequests(1) //并发线程数
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) // 重试补偿策略
                .build();
        IndexRequest indexRequest1 = new IndexRequest("why_test","type1","4")
                .source(XContentType.JSON,"sex","boy");
        IndexRequest indexRequest2 = new IndexRequest("why_test","type1","4")
                .source(XContentType.JSON,"province","shandong");
        IndexRequest indexRequest3 = new IndexRequest("why_test","type1","4")
                .source(XContentType.JSON,"name","gaomi");
        BulkProcessor bulkProcessor = builder.build();
//        bulkProcessor.add(indexRequest1);
//        bulkProcessor.add(indexRequest2);
        bulkProcessor.add(indexRequest3);
        /*** Bulk Processor end ***/

    }

    /**
     * searchRequest用来完成和搜索文档，聚合，建议等相关的任何操作同时也提供了各种方式来对查询结构的高亮操作
     */
    public static void testSearchRequestAPI(){
        // 基本查询
        // 设置搜索的index
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /*** 创建QueryBuilder对象。***/
        // 方法一 通过构造函数
//        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("name","why");
//        matchQueryBuilder.fuzziness(Fuzziness.AUTO) // 模糊查询
//                         .prefixLength(3) //前缀查询的长度
//                         .maxExpansions(10); //max expansion选项，用来控制模糊查询
        // 方法二 通过QueryBuilders工具类来创建QueryBuilder对象。
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("name","why")
                                            .fuzziness(Fuzziness.AUTO)
                                            .prefixLength(3)
                                            .maxExpansions(10);

        /*** 创建QueryBuilder对象。 ***/
        // 设置搜索，可以是任何类型的QueryBuilder
        searchSourceBuilder
                .query(QueryBuilders.termQuery("name","why"))
//                .query(QueryBuilders.matchAllQuery())// 添加match all 查询
//                .query(queryBuilder)
                .from(0) // 起始index
                .size(5) // 大小size
                .timeout(new TimeValue(60, TimeUnit.SECONDS)); // 设置搜索超时时间

        // 将SearchSourceBuilder添加到SearchRequest中
        searchRequest.source(searchSourceBuilder)
//                .searchType("type1") // 设置搜索的type
                .routing("4"); // 设置routing 参数
//                .preference("_local_"); // 配置搜索时偏爱使用本地分片，默认是随机分片
        /*
            SearchSourceBuilder 允许添加一个或多个SortBuilder实例。
            这里包含4钟特殊的实现(Field-,Score,GeoDistance-和ScripSSortBuilder)
         */
        //按照分数_score降序排列(默认行为)
//        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
//        searchSourceBuilder.sort(new FieldSortBuilder("_uid").order(SortOrder.ASC))// 根据id升序排列
//                           .fetchSource(false); //过滤数据源
        // 通过使用通配符模式以更细的粒度包含或排除特定的字段：
//        String[] incluedeFields = new String[] {"title","name","innerObject"};
//        String[] excludeFields = new String[]{"_type1"};
//        searchSourceBuilder.fetchSource(incluedeFields,excludeFields);
//        // 高亮请求设置
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        //字段高亮
//        HighlightBuilder.Field highLightTitle = new HighlightBuilder.Field("name");
//        // 配置高亮类型
//        highLightTitle.highlighterType("unified");
//        //添加到build
//        highlightBuilder.field(highLightTitle);
        // 对请求和聚合分析
//        searchSourceBuilder.profile(false);
        /*** searchResponse ***/
        try{
            SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
            /*** 请求本身的信息 ***/
            // HTTP 状态码
            RestStatus status = searchResponse.status();
            // 查询占用的时间
            TimeValue took = searchResponse.getTook();
            // 是否由于SearchSourceBuilder中设置terminnateAfter而过早终止
            Boolean terminnateEearly = searchResponse.isTerminatedEarly();
            // 是否超时
            boolean timeOut = searchResponse.isTimedOut();
            /*** 请求本身的信息 ***/

            /*** 查询影响的分片数量的统计信息，成功和失败的分片 ***/
            int totalShards = searchResponse.getTotalShards();
            int successfulShards = searchResponse.getSuccessfulShards();
            int failShards = searchResponse.getFailedShards();
            for (ShardSearchFailure failure : searchResponse.getShardFailures()){
                // handle failures
            }
            /*** 查询影响的分片数量的统计信息，成功和失败的分片 ***/

            /*** 检索SearchHits ***/
            SearchHits hits = searchResponse.getHits();
            // 查询命中的数量
            long totalHits = hits.getTotalHits();
            // 最大分值
            float maxScore = hits.getMaxScore();
            // 查询的结果前台在serchHits中，可以通过遍历获取
            SearchHit[] searchHits = hits.getHits();
            for(SearchHit hit : searchHits){
                Map<String,Object> sourceAsMap = hit.getSourceAsMap();
                System.out.println(sourceAsMap);
            }

            /*** 检索SearchHits ***/


        }catch (IOException e){
            e.printStackTrace();
        }
        /*** searchResponse ***/


    }

    public static void fullTextQueries(){

    }
}
