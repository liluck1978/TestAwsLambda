package com.sample.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.config.NodeEndPoint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by Daisuke on 2017/02/05.
 */
public interface Handler <T>{

    String ENDPOINT = System.getenv("CLUSTER_ENDPOINT");
    Integer CLUSTER_PORT = Integer.parseInt(System.getenv("CLUSTER_PORT"));
    Integer TTL = Integer.parseInt(System.getenv("TTL"));

    default Object handleRequest(S3Event s3Event, Context context) {
        MemcachedClient client = null;
        LambdaLogger logger = context.getLogger();

        S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);
        String bucketName = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey();

        logger.log(bucketName);
        logger.log(key);

        try {
            client = new MemcachedClient(new InetSocketAddress(ENDPOINT, CLUSTER_PORT));
            for (NodeEndPoint node : client.getAllNodeEndPoints()) {
                logger.log(node.getHostName());
            }
            List<T> rows = getRowsFromS3(bucketName,key);
            set(client, rows);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.shutdown();
            }
            return null;
        }
    }

    void set(MemcachedClient client, List<T> rows);
    List<T> getRowsFromS3(String bucketName, String key) throws IOException;
}
