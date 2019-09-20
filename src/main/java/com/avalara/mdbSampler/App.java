package com.avalara.mdbSampler;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.RawBsonDocument;
import java.util.UUID;

/**
 * Hello world!
 *
 */
public class App extends AbstractJavaSamplerClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private static final String CONNECTION_STRING = "ConnectionString";
    private static final String DATABASE_NAME = "DatabaseName";
    private static final String COLLECTION_NAME = "CollectionName";
    private static final String DOCUMENT_SIZE = "DocumentSizeInKb";

    private MongoClient mongoClient = null;
    private MongoCollection<Document> collection = null;
    private Document document = null;
    private int docSize = 1;

    @Override
    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();

        defaultParameters.addArgument(CONNECTION_STRING, "");
        defaultParameters.addArgument(DATABASE_NAME, "");
        defaultParameters.addArgument(COLLECTION_NAME, "");
        defaultParameters.addArgument(DOCUMENT_SIZE, "1");

        return defaultParameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {

        String connectionString = context.getParameter(CONNECTION_STRING);
        String databaseName = context.getParameter(DATABASE_NAME);
        String collectionName = context.getParameter(COLLECTION_NAME);
        docSize = Integer.valueOf(context.getParameter(DOCUMENT_SIZE));

        MongoClientURI uri = new MongoClientURI(connectionString);
        mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);

    }

    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.sampleStart();
        try {

            document = new Document("_id", UUID.randomUUID().toString());

            for (int i = 0; i < docSize; i++) {
                document.append("f" + i, createData(1100));
            }

            StopWatch watch = new StopWatch();
            watch.start();

            collection.insertOne(document);

            watch.stop();

            RawBsonDocument rawBsonDocument = RawBsonDocument.parse(document.toJson());

            sampleResult.setLatency(watch.getTime());
            sampleResult.setSentBytes(rawBsonDocument.getByteBuffer().remaining() / 1024);
            sampleResult.sampleEnd();
            sampleResult.setSuccessful(Boolean.TRUE);
            sampleResult.setResponseCodeOK();
            sampleResult.setResponseMessage("Success");

        } catch (Exception e) {
            LOGGER.error("Request was not successfully processed", e);
            sampleResult.sampleEnd();
            sampleResult.setResponseMessage(e.getMessage());
            sampleResult.setSuccessful(Boolean.FALSE);
        }
        return sampleResult;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private static String createData(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i = 0; i < msgSize; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

}
