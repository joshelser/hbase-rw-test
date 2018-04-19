package com.github.joshelser.hbase;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadWriteExample {
  private static final TableName tableName = TableName.valueOf("my_test");
  private static final byte[] family = Bytes.toBytes("f1");

  public static void main(String[] args) throws IOException {
    final Configuration conf = HBaseConfiguration.create();
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin()) {
//      createTable(admin, tableName, family);

//      long numRows = 1_000_000;
//      writeRows(conn, tableName, numRows);
//      readRows(conn, tableName, numRows);
      
      massiveMultiGet(conn, tableName, 15_000_000);
    }
  }

  private static void createTable(Admin admin, TableName tableName, byte[] family) throws IOException {
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(family));
    admin.createTable(desc);
  }

  private static void writeRows(Connection conn, TableName tableName, long numRows) throws IOException {
    try (BufferedMutator mutator = conn.getBufferedMutator(tableName)) {
      byte[] qual = Bytes.toBytes("qual");
      for (long i = 0; i < numRows; i++) {
        Put p = new Put(Bytes.toBytes(i));
        p.addColumn(family, qual, Bytes.toBytes(Long.toString(i)));
        mutator.mutate(p);
      }
    }
  }

  private static void readRows(Connection conn, TableName tableName, long numRows) throws IOException {
    try (Table table = conn.getTable(tableName);
        ResultScanner scanner = table.getScanner(new Scan())) {
      long observedRows = 0;
      for (Result result : scanner) {
        if (result.size() != 1) {
          throw new RuntimeException("Invalid data: " + result);
        }
        observedRows++;
      }
      if (observedRows != numRows) {
        throw new RuntimeException("Expected " + numRows + " rows, but saw " + observedRows);
      }
    }
  }

  private static void massiveMultiGet(Connection conn, TableName tableName, int numGets) throws IOException {
    try (Table table = conn.getTable(tableName)) {
      Get g = new Get(Bytes.toBytes(10));
      Get[] gets = new Get[numGets];
      Arrays.fill(gets, g);

      table.get(Arrays.asList(gets));
    }
  }
}
