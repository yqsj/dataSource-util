package com.yiqi.mongo.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Filter;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-29
 */
public class MongoUtil {
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;

    public MongoUtil(String serveIp, Integer port, String userName, String dataBaseName, String passwd){
        List<ServerAddress> serverAddresses = new ArrayList<>();
        // 创建server地址
        ServerAddress serverAddress = new ServerAddress(serveIp, port);
        serverAddresses.add(serverAddress);
        List<MongoCredential> credentials = new ArrayList<>();
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(userName, dataBaseName, passwd.toCharArray());
        credentials.add(mongoCredential);
        this.mongoClient = new MongoClient(serverAddresses, credentials);
        this.mongoDatabase = mongoClient.getDatabase(dataBaseName);
    }

    public MongoUtil(String serveIp, Integer port, String dataBaseName){
        MongoClient mongoClient = new MongoClient(serveIp,port);
        this.mongoDatabase = mongoClient.getDatabase(dataBaseName);
    }

    /**
     * 获取集合
     * @param collectionName 集合名称
     * @return
     */
    public MongoCollection<?> getCollection(String collectionName){
        // 获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        return collection;
    }

    /**
     * 插入一个文档
     * @param collectionName 文档名称
     * @param document 文档
     */
    public void insertOneDoc(String collectionName,Document document){
        MongoCollection mongoCollection = getCollection(collectionName);
        mongoCollection.insertOne(document);
    }

    /**
     * 插入多个文档
     * @param collectionName 文档名称
     * @param documents 文档
     */
    public void insertMany(String collectionName,List<Document> documents){
        MongoCollection mongoCollection = getCollection(collectionName);
        mongoCollection.insertMany(documents);
    }

    public void deleteOne(String collectionName, Bson filter){
        MongoCollection mongoCollection = mongoDatabase.getCollection(collectionName);
        mongoCollection.deleteOne(filter);
    }

    public void deleteMany(String collectionName, Bson filter){
        MongoCollection mongoCollection = mongoDatabase.getCollection(collectionName);
        mongoCollection.deleteMany(filter);
    }

    public void updateOne(String collectionName,Bson filter,Document document){
        MongoCollection mongoCollection = mongoDatabase.getCollection(collectionName);
        mongoCollection.updateOne(filter,document);
    }

    public void updateMany(String collectionName,Bson filter,Document document){
        MongoCollection mongoCollection = mongoDatabase.getCollection(collectionName);
        mongoCollection.updateOne(filter,document);
    }

    public void find(String collectionName){
        MongoCollection mongoCollection = getCollection(collectionName);
        FindIterable findIterable = mongoCollection.find();
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()){
            System.out.println(cursor.next());
        }
    }

    public void filterFind(String collectionName,Bson filter){
        MongoCollection mongoCollection = mongoDatabase.getCollection(collectionName);
        FindIterable findIterable = mongoCollection.find(filter);
    }

}
