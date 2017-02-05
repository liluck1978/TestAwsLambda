package com.sample.lambda;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.sample.lambda.model.AccountId;
import net.spy.memcached.MemcachedClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Daisuke on 2017/02/04.
 */
public class S3ToCacheHandler implements Handler<AccountId> {

    public void set(MemcachedClient client, List<AccountId> rows) {
        for (AccountId row : rows) {
            client.set(row.getAccountId(), TTL,  System.currentTimeMillis());
        }
    }

    public List<AccountId> getRowsFromS3(String bucketName, String key) throws IOException {
        AmazonS3 s3 = new AmazonS3Client();
        List<AccountId> rows = new ArrayList<>();

        try(S3Object obj = s3.getObject(new GetObjectRequest(bucketName, key))){
            S3ObjectInputStream stream = obj.getObjectContent();
            try(BufferedReader br = new BufferedReader(new InputStreamReader(stream, "UTF-8"))){
                br.lines().forEach(l -> {
                    AccountId aid = new AccountId();
                    aid.setAccountId(l);
                    rows.add(aid);
                });
            }
        }finally {
            return rows;
        }
    }
}

