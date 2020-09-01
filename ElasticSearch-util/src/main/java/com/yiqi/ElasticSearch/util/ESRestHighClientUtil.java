package com.yiqi.ElasticSearch.util;

import com.yiqi.ElasticSearch.bean.IndexBean;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-06
 */
public class ESRestHighClientUtil {
    // client
    private static RestHighLevelClient restHighLevelClient;

    public ESRestHighClientUtil(String esIp, Integer port, String scheme) {
        this.createClient(esIp, port, scheme);
    }

    /**
     * 创建es的连接
     * @param esIp es服务器ip
     * @param port 端口
     * @param scheme 协议
     */
    public void createClient(String esIp,Integer port,String scheme){
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(esIp,port,scheme)));
    }

    /**
     * 创建IndexRequest
     * @param index 索引
     * @param indexType type
     * @param docId id
     * @param dataMap dataMap
     */
    public IndexRequest createIndexRequest(String index, String indexType, String docId, Map<String,Object> dataMap){
        IndexRequest indexRequest;
        if(null == index || null == indexType){
            throw new ElasticsearchException("index或者indexType不能为空");
        }
        if(null == docId){
            indexRequest = new IndexRequest(index,indexType);
        }else {
            indexRequest = new IndexRequest(index,indexType,docId);
        }
        return indexRequest;
    }

    /**
     * 同步执行index
     * @param index 索引
     * @param indexType type
     * @param docId id
     * @param dataMap dataMap
     * @return
     * @throws IOException
     */
    public IndexResponse execIndex(String index,String indexType,String docId,Map<String,Object> dataMap) throws IOException {
        return restHighLevelClient.index(createIndexRequest(index,indexType,docId,dataMap).source(dataMap),RequestOptions.DEFAULT);
    }

    /**
     * 同步执行get
     * @param index 索引
     * @param indexType type
     * @param docId id
     * @param includes 返回需要包含的字段，可以传入空
     * @param excludes 返回需要不包含的字段，可以传入空
     * @param version version
     * @param versionType versionType
     * @return
     * @throws IOException
     */
    public GetResponse execGet(String index, String indexType, String docId, String[] includes, String[] excludes, Integer version, VersionType versionType) throws IOException{
        if(null == includes || includes.length == 0){
            includes = Strings.EMPTY_ARRAY;
        }
        if(null == excludes || excludes.length == 0){
            excludes = Strings.EMPTY_ARRAY;
        }
        GetRequest getRequest = new GetRequest(index,indexType,docId);
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true,includes,excludes);
        getRequest.realtime(true);
        if(null != version){
            getRequest.version(version);
        }
        if(null != versionType){
            getRequest.versionType(versionType);
        }
        return restHighLevelClient.get(getRequest,RequestOptions.DEFAULT);
    }

    /**
     * 同步执行get
     * @param index 索引
     * @param indexType type
     * @param docId id
     * @return
     * @throws IOException
     */
    public GetResponse execGet(String index,String indexType,String docId) throws IOException{
        GetRequest getRequest = new GetRequest(index,indexType,docId);
        return restHighLevelClient.get(getRequest,RequestOptions.DEFAULT);
    }

    /**
     * 查询文档是否存在
     * @param index 索引
     * @param indexType 类型
     * @param docId 文档id
     * @return
     * @throws IOException
     */
    public Boolean ExitDoc(String index,String indexType,String docId) throws IOException{
        GetRequest getRequest = new GetRequest(index,indexType,docId);
        getRequest.fetchSourceContext(new FetchSourceContext(false)).storedFields("_none_");
        return restHighLevelClient.exists(getRequest,RequestOptions.DEFAULT);
    }

    /**
     * 同步执行del
     * @param index 索引
     * @param indexType type
     * @param docId docId
     * @param timeValue 超时时间
     * @param refreshPolicy 刷新策略
     * @param version 版本
     * @param versionType 版本类型
     * @return DeleteResponse
     * @throws IOException
     */
    public DeleteResponse execDelete(String index, String indexType,String docId, TimeValue timeValue, WriteRequest.RefreshPolicy refreshPolicy,Integer version,VersionType versionType) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index,indexType,docId);
        if(null != timeValue){
            deleteRequest.timeout(timeValue);
        }
        if(null != refreshPolicy){
            deleteRequest.setRefreshPolicy(refreshPolicy);
        }
        if(null != version){
            deleteRequest.version(version);
        }
        if (null != versionType){
            deleteRequest.versionType(versionType);
        }
        return restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
    }

    /**
     * 同步执行delete
     * @param index 索引
     * @param indexType type
     * @param docId 文档id
     * @return
     * @throws IOException
     */
    public DeleteResponse execDelete(String index,String indexType,String docId) throws IOException{
        return restHighLevelClient.delete(new DeleteRequest(index,indexType,docId),RequestOptions.DEFAULT);
    }

    /**
     * 同步执行update
     * @param index 索引
     * @param indexType type
     * @param docId docId
     * @param dataMap  dataMap
     * @param timeValue 超时时间
     * @param refreshPolicy 刷新策略
     * @param version 版本
     * @param versionType 版本类型
     * @param docAsUpsert true 如果设置文档不存在，就必须要创建一个
     * @param includes 包括特定字段
     * @param excludes 排除特定字段
     * @return
     * @throws IOException
     */
    public UpdateResponse execUpdate(String index,String indexType,String docId,Map<String,Object> dataMap,TimeValue timeValue,
                                     WriteRequest.RefreshPolicy refreshPolicy,Integer version,VersionType versionType,Boolean docAsUpsert,
                                     String[] includes,String[] excludes) throws IOException{
        UpdateRequest updateRequest = new UpdateRequest(index,indexType,docId);
        updateRequest.doc(dataMap).docAsUpsert(docAsUpsert)
                .retryOnConflict(3);// 冲突时重试的次数
        if (null != timeValue){
            updateRequest.timeout(timeValue);
        }
        if(null != refreshPolicy){
            updateRequest.setRefreshPolicy(refreshPolicy);
        }
        if(null != version){
            updateRequest.version(version);
        }
        if(null != versionType){
            updateRequest.versionType(versionType);
        }
        if(null == includes && null == excludes){
            return restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
        }else {
            if(null == includes || includes.length == 0){
                includes = Strings.EMPTY_ARRAY;
            }
            if(null == excludes || excludes.length == 0){
                excludes = Strings.EMPTY_ARRAY;
            }
            return restHighLevelClient.update(updateRequest.fetchSource(new FetchSourceContext(true,includes,excludes)),RequestOptions.DEFAULT);
        }
    }

    /**
     * 同步执行update
     * @param index 索引
     * @param indexType type
     * @param docId docId
     * @param docAsUpsert 如果为true则不存在就插入，若为false则存在才更新
     * @param dataMap dataMap
     * @return
     * @throws IOException
     */
    public UpdateResponse execUpdate(String index,String indexType,String docId,Boolean docAsUpsert ,Map<String,Object> dataMap)throws IOException{
        return execUpdate(index, indexType, docId, dataMap,null,null,null,null,docAsUpsert,null,null);
    }

    /**
     * 对List<IndexBean>进行处理
     * @param indexBeans List<IndexBean>
     * @return
     */
    private BulkRequest getBulkRequest(List<IndexBean> indexBeans){
        BulkRequest bulkRequest = new BulkRequest();
        indexBeans.forEach(indexBean -> {
            switch (indexBean.getOperationType()){
                case "0":
                    bulkRequest.add(null != indexBean.getId() ?
                            new IndexRequest(indexBean.getIndex(),indexBean.getType(),indexBean.getId()) :
                            new IndexRequest(indexBean.getIndex(),indexBean.getType()));
                case "1":
                    if(null != indexBean.getId()){
                        throw new ElasticsearchException("doc不能为空看");
                    }
                    bulkRequest.add(new UpdateRequest(indexBean.getIndex(), indexBean.getType(), indexBean.getId()));
                case "2":
                    if(null != indexBean.getId()){
                        throw new ElasticsearchException("doc不能为空看");
                    }
                    bulkRequest.add(new DeleteRequest(indexBean.getIndex(), indexBean.getType(), indexBean.getId()));
                default:
                    throw new ElasticsearchException("operationType不存在");
            }

        });
        return bulkRequest;
    }

    /**
     * 批量操作
     * @param indexBeanList indexBean的集合
     * @param timeValue 超时时间
     * @param refreshPolicy 刷新策略
     * @return
     * @throws IOException
     */
    public BulkResponse execBulk(List<IndexBean> indexBeanList,TimeValue timeValue,WriteRequest.RefreshPolicy refreshPolicy)throws IOException{
        BulkRequest bulkRequest = getBulkRequest(indexBeanList);
        if(null != timeValue){
            bulkRequest.timeout(timeValue);
        }
        if (null != refreshPolicy){
            bulkRequest.setRefreshPolicy(refreshPolicy);
        }
        return restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
    }

    /**
     * 同步执行bulk
     * @param indexBeans
     * @return
     * @throws IOException
     */
    public BulkResponse execBulk(List<IndexBean> indexBeans) throws IOException{
        return execBulk(indexBeans,null,null);
    }

    /**
     * create searchRequest
     * @param index
     * @param indexType
     * @return
     */
    public SearchRequest createSearch(String index,String indexType){
        SearchRequest searchRequest;
        if(null == index){
            throw new ElasticsearchException("index不能为空");
        }
        if(null == indexType){
            searchRequest = new SearchRequest(index,indexType);
        }else{
            searchRequest = new SearchRequest(index);
        }
        return searchRequest;
    }

    /**
     * createSearchSourceBuilder
     * @param from
     * @param size
     * @param termsQueryBuilder
     * @param sortField
     * @param sortBuilder SortBuilder是一个抽象类，有4个子类
     * org.elasticsearch.search.sort.FieldSortBuilder 根据某属性值排序
     * org.elasticsearch.search.sort.GeoDistanceSortBuilder 根据地理位置排序
     * org.elasticsearch.search.sort.ScoreSortBuilder 根据score排序
     * org.elasticsearch.search.sort.ScriptSortBuilder 根据自定义脚本排序
     * @param fetchSource
     * @return
     */
    public SearchSourceBuilder createSearchSourceBuilder( Integer from, Integer size, TermsQueryBuilder termsQueryBuilder,
                                                          String sortField, SortBuilder sortBuilder,Boolean fetchSource){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        if(null != termsQueryBuilder){
            searchSourceBuilder.query(termsQueryBuilder);
        }

        if(null != sortField){
            searchSourceBuilder.sort(sortField);
        }

        if(null != sortBuilder){
            searchSourceBuilder.sort(sortBuilder);
        }

        if(null != fetchSource){
            searchSourceBuilder.fetchSource(fetchSource);
        }
        searchSourceBuilder.from(from).size(size).timeout(new TimeValue(120, TimeUnit.SECONDS));
        if(null != fetchSource){
            searchSourceBuilder.fetchSource(fetchSource);
        }
        return searchSourceBuilder;
    }

    /**
     * 同步执行Search
     * @param index
     * @param indexType
     * @return
     * @throws IOException
     */
    public SearchResponse execSearch(String index,String indexType) throws IOException{
        return restHighLevelClient.search(createSearch(index, indexType),RequestOptions.DEFAULT);
    }

    /**
     * 同步执行Search
     * @param index 索引
     * @param indexType type
     * @param from
     * @param size
     * @param termsQueryBuilder
     * @return
     * @throws IOException
     */
    public SearchResponse execSearch(String index, String indexType, Integer from, Integer size,
                                     TermsQueryBuilder termsQueryBuilder)throws IOException{
        return restHighLevelClient.search(createSearch(index,indexType)
                .source(createSearchSourceBuilder(from,size,termsQueryBuilder,null,null,null)),RequestOptions.DEFAULT);
    }

    /**
     *
     * @param index
     * @param indexType
     * @param from
     * @param size
     * @param termsQueryBuilder
     * @param matchQueryBuilder
     * @return
     * @throws IOException
     */
    public SearchResponse execSearch(String index,String indexType,Integer from,Integer size,
                                     TermsQueryBuilder termsQueryBuilder,MatchQueryBuilder matchQueryBuilder) throws IOException{
        if(null == matchQueryBuilder){
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index,indexType)
                .source(createSearchSourceBuilder(from,size,termsQueryBuilder,null,null,null)
                        .query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }

    public SearchResponse execSearch(String index,String indexType,Integer from,Integer size,MatchQueryBuilder matchQueryBuilder)throws IOException{
        if(null == matchQueryBuilder){
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index,indexType)
                .source(createSearchSourceBuilder(from,size,null,null,null,null).query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }

    public SearchResponse execSearch(String index, String indexType, Integer from, Integer size, MatchQueryBuilder matchQueryBuilder,String sortField) throws IOException{
        if(null == matchQueryBuilder){
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index,indexType).source(createSearchSourceBuilder(from,size,null,sortField,
                null,null).query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }

    public SearchResponse execSearch(String index,String indexType,Integer from,Integer size,MatchQueryBuilder matchQueryBuilder,String sortField,Boolean fetchSource) throws IOException{
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index, indexType).source(createSearchSourceBuilder(from, size, null, sortField, null,
                fetchSource).query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }

    public SearchResponse execSearch(String index,String indexType,Integer from,Integer size,MatchQueryBuilder matchQueryBuilder,SortBuilder sortBuilder) throws IOException{
        if(null == matchQueryBuilder){
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index,indexType).source(createSearchSourceBuilder(from,size,null,null,
                sortBuilder,null).query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }

    public SearchResponse execSearch(String index, String indexType, Integer from, Integer size, TermsQueryBuilder termsQueryBuilder, MatchQueryBuilder matchQueryBuilder, String sortField) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index, indexType).source(createSearchSourceBuilder(from, size, termsQueryBuilder, sortField, null,
                null).query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }

    public SearchResponse execSearch(String index, String indexType, Integer from, Integer size, TermsQueryBuilder termsQueryBuilder, MatchQueryBuilder matchQueryBuilder, SortBuilder sortBuilder, Boolean fetchSource) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index, indexType).source(createSearchSourceBuilder(from, size, termsQueryBuilder, null, sortBuilder,
                fetchSource).query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }

    public SearchResponse execSearch(String index, String indexType, Integer from, Integer size, TermsQueryBuilder termsQueryBuilder, MatchQueryBuilder matchQueryBuilder, SortBuilder sortBuilder) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index, indexType).source(createSearchSourceBuilder(from, size, termsQueryBuilder, null,
                sortBuilder, null).query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }

    public SearchResponse execSearch(String index, String indexType, Integer from, Integer size, TermsQueryBuilder termsQueryBuilder, MatchQueryBuilder matchQueryBuilder, String sortField, Boolean fetchSource) throws IOException {
        if (null == matchQueryBuilder) {
            throw new ElasticsearchException("matchQueryBuilder为null");
        }
        return restHighLevelClient.search(createSearch(index, indexType).source(createSearchSourceBuilder(from, size, termsQueryBuilder, sortField, null,
                fetchSource).query(matchQueryBuilder)),RequestOptions.DEFAULT);
    }
    /**
     * 获取当前的restHighLevelClient
     * @return RestHighLevelClient
     */
    public RestHighLevelClient getRestHighLevelClient(){
        return restHighLevelClient;
    }

    /**
     * 将SearchRespose中的数据处理成List<Map<String,Object>>
     * @param response
     * @return
     */
    public List<Map<String,Object>> getMapFromSearchRes(SearchResponse response){
        List<Map<String,Object>> result = new ArrayList<>();
        SearchHits searchHits = response.getHits();
        SearchHit[] hits = searchHits.getHits();
        Arrays.stream(hits).forEach(i->{
            Map<String,Object> map = i.getSourceAsMap();
            result.add(map);
        });
        return result;
    }


}
