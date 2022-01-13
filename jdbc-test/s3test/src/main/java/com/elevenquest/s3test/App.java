package com.elevenquest.s3test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3EncryptionClientV2Builder;
import com.amazonaws.services.s3.AmazonS3EncryptionV2;
import com.amazonaws.services.s3.model.CryptoConfigurationV2;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Hello world!
 *
 */
public class App 
{
    ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(/* 'default' */);  

    public static void main( String[] args )
    {
        String kmskey = "";
        String uri = "";
        if(args.length > 2) {
            kmskey = args[0];
            uri = args[1];
        } else {
            kmskey = System.getenv("KMS_KEY_ID");
            uri = System.getenv("TEST_URI");
            System.out.println(kmskey + ":" + uri);
        }
        App testApp = new App();
        testApp.testS3CSEEncryptedFile(kmskey, uri); 
    }

    public void testS3CSEEncryptedFile(String kmskeyid, String uri) {
        AmazonS3EncryptionV2 s3Crypto = AmazonS3EncryptionClientV2Builder.standard()
        .withRegion(Regions.AP_NORTHEAST_2)
        .withCredentials(credentialsProvider)
        .withCryptoConfiguration(new CryptoConfigurationV2().withCryptoMode(CryptoMode.AuthenticatedEncryption))
        .withEncryptionMaterialsProvider(new KMSEncryptionMaterialsProvider(kmskeyid))
        .build();
        String filepath = (uri.indexOf("s3://") == 0) ? uri.substring("s3://".length()) : uri;
        String bucket =  filepath.substring(0,filepath.indexOf("/"));
        String key = filepath.substring(bucket.length()+1);
        GetObjectRequest req = new GetObjectRequest(bucket, key);
        S3Object object = s3Crypto.getObject(req);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(object.getObjectContent())));) {
            int cnt = 0;
            String aLine = null;
            while ((aLine = br.readLine()) != null) {
                System.out.println(aLine);
                cnt++;
                if(cnt > 10)
                    break;
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();            
        }
        s3Crypto.shutdown();
    }
}
