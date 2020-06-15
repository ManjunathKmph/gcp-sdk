package com.manju.gcp.bigtable;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.threeten.bp.Duration;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class BigTableService {

  private BigtableDataClient bigtableDataClient;

  public BigTableService(String projectId, String instanceId) {
      try {
          RetrySettings retrySettings =
                  RetrySettings.newBuilder()
                          .setInitialRetryDelay(Duration.ofMillis(5))
                          .setRetryDelayMultiplier(2.0)
                          .setMaxRetryDelay(Duration.ofSeconds(600))
                          .setTotalTimeout(Duration.ofSeconds(10))
                          .setInitialRpcTimeout(Duration.ofSeconds(10))
                          .setMaxRpcTimeout(Duration.ofSeconds(10))
                          .setMaxAttempts(5)
                          .build();
          BigtableDataSettings settings = BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
          settings.getStubSettings().mutateRowSettings().toBuilder().setRetrySettings(retrySettings);
          bigtableDataClient = BigtableDataClient.create(settings);

      } catch (Exception ex) {
          ex.printStackTrace();
          throw new RuntimeException(ex);
      }
  }
  
  public void writeDataToTable(String tableName, String rowKey, String columnFamily, String qualifier, String value,
                               Optional<Long> cellTimeStamp) {
      try {
          validateIsEmtpy(tableName, "Table");
          validateIsEmtpy(rowKey, "Row Key");
          validateIsEmtpy(qualifier, "Qualifier");
          validateIsEmtpy(columnFamily, "Column Family");
          RowMutation rowMutation = null;
          if(cellTimeStamp.isPresent()) {
              RowMutation.create(tableName, rowKey).setCell(columnFamily, qualifier, cellTimeStamp.get().longValue(), value);
          } else {
              RowMutation.create(tableName, rowKey).setCell(columnFamily, qualifier, value);
          }
          bigtableDataClient.mutateRow(rowMutation);
      } catch(Exception ex) {
          ex.printStackTrace();
      }
  }

  public void addCell(Mutation mutation, String columnFamily, ByteString[] qualifiers, ByteString[] values) {
      validateIsEmtpy(columnFamily, "Column Family");
      validateQualifiersAndValues(qualifiers, values);
      if(qualifiers.length == 0 && values.length == 0) {
          System.out.println("Blank qualifiers and values, hence skipping it.");
          return;
      }
      for(int i=0; i < qualifiers.length; i++) {
          mutation.setCell(columnFamily, qualifiers[i], values[i]);
      }
  }

  public void saveMutationSync(String tableName, String rowKey, Mutation mutation) {
      BulkMutation bulkMutation = BulkMutation.create(tableName);
      bulkMutation.add(rowKey, mutation);
      bigtableDataClient.bulkMutateRows(bulkMutation);
  }

  public void saveMutationAsync(String rowKey, Mutation mutation, String tableName) {
      BulkMutation bulkMutation = BulkMutation.create(tableName);
      bulkMutation.add(rowKey, mutation);
      ApiFuture<Void> apiFuture = bigtableDataClient.bulkMutateRowsAsync(bulkMutation);
      ApiFutures.addCallback(
              apiFuture,
              new ApiFutureCallback<Void>() {
                  public void onSuccess(Void noMessage) {
                      System.out.println("Saved record into Bigtable");
                  }
                  public void onFailure(Throwable t) {
                      System.out.println("Error while saving record to bigtable asynchronous" + mutation.toString());
                  }
              }, MoreExecutors.directExecutor()
      );
  }

  private void validateQualifiersAndValues(ByteString[] qualifiers, ByteString[] values) {
      if(qualifiers == null) {
          throw new IllegalArgumentException("Qualifiers cannot be empty or null.");
      }
      if(values == null) {
          throw new IllegalArgumentException("Values cannot be empty or null.");
      }
      if(qualifiers.length != values.length) {
          throw new IllegalArgumentException("Qualifiers and Values are dependent each othere, hence it has to be same size.");
      }
  }

  public Row readSingleRowWithExactRowKey(String tableName, String rowKey) {
      try {
          validateIsEmtpy(tableName, "Table");
          validateIsEmtpy(rowKey, "Row Key");
          return bigtableDataClient.readRow(tableName, rowKey);
      } catch (Exception ex) {
          ex.printStackTrace();
      }
      return null;
  }

  public ServerStream<Row> readMultipleRowsWithExactRowKeys(String tableName, List<String> rowKeys) {
      try {
          validateIsEmtpy(tableName, "Table");
          rowKeys.forEach(rowKey -> validateIsEmtpy(rowKey, "Row Key"));
          Query query = Query.create(tableName).filter(FILTERS.limit().cellsPerColumn(1));;
          rowKeys.forEach(query::rowKey);
          return bigtableDataClient.readRows(query);
      } catch (Exception ex) {
          ex.printStackTrace();
      }
      return null;
  }

  public ServerStream<Row> fetchRowsWithPrefixRowKey(String tableName, String prefixRowKey) {
      try {
          validateIsEmtpy(tableName, "Table");
          validateIsEmtpy(prefixRowKey, "Prefix Row Key");
          Query query = Query.create(tableName).prefix(prefixRowKey).filter(FILTERS.limit().cellsPerColumn(1));
          return bigtableDataClient.readRows(query);
      } catch (Exception ex) {
          ex.printStackTrace();
      }
      return null;
  }

  public boolean isRowKeyPresent(String tableName, String rowKey) {
      try {
          validateIsEmtpy(tableName, "Table");
          validateIsEmtpy(rowKey, "Row Key");
          Row row = bigtableDataClient.readRow(tableName, rowKey);
          if(row != null) {
              return true;
          }
      } catch (Exception ex) {
          ex.printStackTrace();
      }
      return false;
  }

  private void validateIsEmtpy(String value, String type) {
      if(StringUtils.isEmpty(value)) {
          throw new IllegalArgumentException(type + " is required.");
      }
  }

  public void incrementCounter(String tableName, String key, String columnFamily, String columnName) {
      try {
          validateIsEmtpy(tableName, "Table");
          validateIsEmtpy(columnName, "Column Name");
          ReadModifyWriteRow readModifyWriteRow = ReadModifyWriteRow.create(tableName, ByteString.copyFromUtf8(key));
          readModifyWriteRow.increment(columnFamily, columnName, 1);
          bigtableDataClient.readModifyWriteRow(readModifyWriteRow);
      } catch (Exception ex) {
          ex.printStackTrace();
      }
  }
  
  public ServerStream<Row> fetchRowsByRowKeyRegex(String tableName, String rowKeyRegex) {
      try {
          validateIsEmtpy(tableName, "Table");
          Filters.ChainFilter filter = FILTERS.chain().filter(FILTERS.key().regex(rowKeyRegex)).filter(FILTERS.limit().cellsPerColumn(1));
          Query query = Query.create(tableName).filter(filter);
          return bigtableDataClient.readRows(query);
      } catch (Exception ex) {
          throw new RuntimeException(ex);
      }
  }

  public void close() {
      try {
          bigtableDataClient.close();
      } catch (Exception io){}
  }

}
