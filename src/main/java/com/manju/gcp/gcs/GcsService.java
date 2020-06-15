package com.manju.gcp.gcs;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class GcsService {
  
  private Storage storage;

  public GcsService() {
      storage = StorageOptions.getDefaultInstance().getService();
  }

  public void downloadObjects(String bucketName, String keyDirPrefix, String localDirPath) {
      validateBucketAndKeyPrefix(bucketName, keyDirPrefix);
      if (localDirPath == null) {
          throw new IllegalArgumentException("Local Dir Path cannot be null.");
      }
      try {
          Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(keyDirPrefix));
          for (Blob blob : blobs.iterateAll()) {
              if (!blob.getName().endsWith("/")) {
                  Path localPath = Paths.get(localDirPath, blob.getName());
                  Path parent = localPath.getParent();
                  if (!Files.exists(parent))
                      Files.createDirectories(parent);
                  blob.downloadTo(localPath);
              }
          }
      } catch (Exception io) {
          System.err.println("Error occurred while downloading the directory files into local");
          throw new RuntimeException(io);
      }
  }

  public List<String> readAllObjects(String bucketName, String keyDirPrefix) {
      validateBucketAndKeyPrefix(bucketName, keyDirPrefix);
      try {
          List<String> filesData = new ArrayList<>();
          Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(keyDirPrefix));
          for (Blob blob : blobs.iterateAll()) {
              if (blob != null) {
                  filesData.add(new String(blob.getContent()));
              }
          }
          return filesData;
      } catch (Exception io) {
          System.err.println("Error occurred while downloading the directory files into local");
          throw new RuntimeException(io);
      }
  }

  public byte[] readObject(String strBucket, String strKey) {
      validateBucketAndKeyPrefix(strBucket, strKey);
      try {
          BlobId blobId = BlobId.of(strBucket, strKey);
          Blob blob = storage.get(blobId);
          if (blob == null) {
              return null;
          }
          return blob.getContent();
      } catch (Exception io) {
          System.err.println("Error occurred while downloading the directory files into local");
          throw new RuntimeException(io);
      }
  }

  public void renameAllObjects(String bucketName, String keyDirPrefix, String newBucketName) {
      validateBucketAndKeyPrefix(bucketName, keyDirPrefix);
      try {
          Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(keyDirPrefix));
          for (Blob blob : blobs.iterateAll()) {
              if (blob == null) {
                  continue;
              }
              String name = blob.getName();
              String newKeyName = name.replace(".gz", ".json");
              blob.copyTo(newBucketName, newKeyName);
          }
          return;
      } catch (Exception io) {
          System.err.println("Error occurred while downloading the directory files into local");
          throw new RuntimeException(io);
      }
  }

  public String readContent(String strBucket, String strKey) {
      validateBucketAndKeyPrefix(strBucket, strKey);
      try {
          BlobId blobId = BlobId.of(strBucket, strKey);
          Blob blob = storage.get(blobId);
          if (blob == null) {
              return "";
          }
          return new String(blob.getContent());
      } catch (Exception ex) {
          System.err.println("Error in reading the file from GCS, bucket-" + strBucket + "and key prefix-" + strKey);
          throw new RuntimeException(ex);
      }
  }

  public void downloadObject(String bucketName, String keyPrefix, String localDirPath) {
      validateBucketAndKeyPrefix(bucketName, keyPrefix);
      if (localDirPath == null) {
          throw new IllegalArgumentException("Local Dir Path cannot be null.");
      }
      try {
          BlobId blobId = BlobId.of(bucketName, keyPrefix);
          Blob blob = storage.get(blobId);
          Path localPath = Paths.get(localDirPath, blob.getName());
          Path parent = localPath.getParent();
          if (!Files.exists(parent))
              Files.createDirectories(parent);
          blob.downloadTo(localPath);
      } catch (Exception io) {
          System.err.println("Error in downloading file to local path-" + localDirPath);
          throw new RuntimeException(io);
      }
  }

  public Blob uploadObject(String bucketName, String keyPrefix, String content) {
      BlobId blobId = BlobId.of(bucketName, keyPrefix);
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      return storage.create(blobInfo, content.getBytes(StandardCharsets.UTF_8));
  }

  public void uploadToGCP(String bucket, String key, File directory) {
      try {
          BlobId blobId = BlobId.of(bucket, key);
          BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
          storage.create(blobInfo, Files.readAllBytes(directory.toPath()));
      } catch (Exception e) {
          System.err.println("Error while uploading file");
          throw new RuntimeException(e);
      }
  }
      
  public boolean deleteObject(String bucketName, String keyPrefix){
      BlobId blobId = BlobId.of(bucketName, keyPrefix);
      return storage.delete(blobId);

  }
  
  public void setCredential(String credentialJson) throws IOException {
    InputStream result = new ByteArrayInputStream(credentialJson.getBytes(StandardCharsets.UTF_8));
    Credentials credentials = GoogleCredentials.fromStream(result);
    storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
  }
  
  private void validateBucketAndKeyPrefix(String bucketName, String keyPrefix) {
    if (StringUtils.isEmpty(bucketName))
        throw new IllegalArgumentException("Bucket Name cannot be empty or null.");
    if (StringUtils.isEmpty(keyPrefix))
        throw new IllegalArgumentException("Key prefix cannot be empty or null.");
  }

}
