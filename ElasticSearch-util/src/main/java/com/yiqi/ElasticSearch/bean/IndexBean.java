package com.yiqi.ElasticSearch.bean;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-21
 */
public class IndexBean {
    /* 索引 */
    private String index;
    /* 类型 */
    private String type;
    /* ID */
    private String id;
    /* 操作类型 0 indexRequest 1 GetRequest 2 UpdateRequest 3 DeleteRequest*/
    private String operationType;

    public IndexBean(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public IndexBean() {
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }
}
