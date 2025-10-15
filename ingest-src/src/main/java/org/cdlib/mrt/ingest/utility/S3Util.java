/*
Copyright (c) 2011, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************/
package org.cdlib.mrt.ingest.utility;

// import com.amazonaws.AmazonServiceException;
// import com.amazonaws.auth.AWSCredentials;
// import com.amazonaws.auth.AWSStaticCredentialsProvider;
// import com.amazonaws.services.s3.AmazonS3;
// import com.amazonaws.services.s3.AmazonS3Client;
// import com.amazonaws.services.s3.AmazonS3ClientBuilder;
// import com.amazonaws.services.s3.model.S3Object;
// import com.amazonaws.services.s3.model.S3ObjectInputStream;
// import com.amazonaws.auth.BasicAWSCredentials;
// import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
// import com.amazonaws.ClientConfiguration;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.model.S3ResponseMetadata;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.InputStream;
import java.io.File;
import java.net.URL;
import java.net.URI;
import java.net.MalformedURLException;

import org.cdlib.mrt.utility.TException;

/**
 * S3 tool
 * @author mreyes
 */
public class S3Util
{

    private static final String NAME = "S3Util";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;

    private static int node;


    public static S3Client getAWSClient(Region region)
            throws TException
    {
	System.out.println("[S3Util] getAWSClient - Region=" + region.toString());
        try {
            S3Client s3Client = S3Client.builder()
                     .region(region)
                     .forcePathStyle(true)
                     .build();
            return s3Client;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }


    public static S3Client getMinioClient(Region region, String accessKey, String secretKey, String endPoint)
            throws TException
    {
	System.out.println("[S3Util] getMinioClient - Endpoint=" + endPoint);
        try {
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
            AwsCredentialsProvider creds = StaticCredentialsProvider.create(awsCreds);

            S3Client s3Client = S3Client.builder()
                     .credentialsProvider(creds)
                     .region(region)
                     .endpointOverride(URI.create(endPoint))
                     .forcePathStyle(true)
                     .build();
            return s3Client;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
        }
    }




    public static InputStream getObjectSyncInputStream (S3Client s3Client, String bucketName, String keyName)
        throws TException
    {
        System.out.println("[S3Util] getObjectSyncInputStream " 
		+ " - keyName=" + keyName 
		+ " - bucketName=" + bucketName);

        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            InputStream is = s3Client.getObject(objectRequest, ResponseTransformer.toInputStream());
            return is;

        } catch (S3Exception e) {
           //System.err.println(e.awsErrorDetails().errorMessage());
           if ((e.statusCode() == 404) || e.toString().contains("404")) {
               throw new TException.REQUESTED_ITEM_NOT_FOUND("Not found:"
                       + " - bucket:" + bucketName
                       + " - key:" + keyName);
           }
           throw new TException(e);
        }
    }

}
