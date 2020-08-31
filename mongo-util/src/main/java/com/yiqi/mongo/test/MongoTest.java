package com.yiqi.mongo.test;

import com.yiqi.mongo.util.MongoUtil;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-29
 */
public class MongoTest {
    private MongoUtil mongoUtil = new MongoUtil("yqsj",32771,"root","admin","blog","why2020");

    @Test
    public void testInsertOne(){
        Document document = new Document("name","wanghy")
                .append("sex","男")
                .append("age","14");
        mongoUtil.insertOneDoc("eqwrq",document);
    }

    @Test
    public void testInsertMany(){
        List<Document> documents = new ArrayList<>();
        documents.add(new Document("name","xf").append("age","12"));
        documents.add(new Document("name","cq").append("age","16"));
        mongoUtil.insertMany("eqwrq",documents);
    }

    @Test
    public void testFind(){
        mongoUtil.find("eqwrq");
    }


}
