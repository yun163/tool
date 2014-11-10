package com.coinport.tester;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Delete;

import java.util.Date;
import java.text.SimpleDateFormat;

public class TestHBase {
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    HTablePool pool = null;
    String hTableName = null;
    final String FAMILY = "d";
    HTableInterface hTable = null;

    public TestHBase(String zookeeper, String hTableName) {
        try {
            String[] zookeeperIpPort = zookeeper.split(":");
            Configuration hConf = HBaseConfiguration.create();
            hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, zookeeperIpPort[0]);
            hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, zookeeperIpPort[1]);
            if (!checkTable(hConf, hTableName)) {
                return;
            }
            this.pool = new HTablePool(hConf, 10);
            this.hTableName = hTableName;
        } catch (Exception e) {
        }
    }

    public boolean getHTable () {
        this.hTable = pool.getTable(hTableName);
        if (hTable == null) return false;
        else return true;
    }

    public void closeHTable () throws Exception {
        this.hTable.close();
    }

    private String renderResult(String column, byte[] value) {
        if (column == null || column.isEmpty() || value == null || value.length == 0) {
            return "";
        }
        if ("thrift".equals(column)) {
            return "123";
        } else if ("expire".equals(column)) {
            long expire = Bytes.toLong(value);
            return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(expire));
        } else {
            return Bytes.toString(value);
        }
    }

    public void getRow(String key, String type, String column) {
        getHTable();
        try {
            if (hTable == null) {
                System.out.println("failed to get interface for : " + hTableName);
                return;
            }
            Get columGetter = new Get(getKey(key, type)).addColumn(Bytes.toBytes("d"), Bytes.toBytes(column));
            Result result = hTable.get(columGetter);
            closeHTable();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (hTable != null) {
                try {
                    hTable.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public void delRow(String key, String type) {
        try {
            getHTable();
            if (hTable == null) {
                System.out.println("failed to get interface for : " + hTableName);
                return;
            }
            Delete deleter = new Delete(getKey(key, type)).deleteFamily(Bytes.toBytes("d"));
            hTable.delete(deleter);
            System.out.println("Successfully delete");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                hTable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean checkTable(Configuration hConf, String tableName) {
        boolean res = false;
        try {
            HBaseAdmin admin = new HBaseAdmin(hConf);
            if (tableName.length() == 0) {
                System.err.println("Invalid hTable name");
                return res;
            }
            HTableDescriptor[] descriptors = admin.listTables(tableName);
            if (descriptors.length == 0) {
                HTableDescriptor descriptor = new HTableDescriptor(tableName);
                HColumnDescriptor family = new HColumnDescriptor("d").setInMemory(false).setMaxVersions(1);
                descriptor.addFamily(family);
                admin.createTable(descriptor);
                admin.close();
            }
            return true;
        } catch (Exception e) {
            System.out.println(">>>>>>>>>>>>>> Fail to checkTable");
            e.printStackTrace();
        }
        return false;
    }

    private byte[] getKey(String key, String type) {
        if ("0".equals(type)) {
            return Bytes.toBytes(key);
        } else if ("1".equals(type)) {
            return Bytes.toBytes("12341234");
        } else {
            return null;
        }
    }

    private void close() throws Exception {
        pool.close();
    }

    private void putRow(String column, String value, String key) throws Exception {
        Put put = new Put(key.getBytes());
        put.add(FAMILY.getBytes(), column.getBytes(), value.getBytes());
        hTable.put(put);
    }

    private void batchPut() throws Exception {
        if (!getHTable()) {
            System.err.println("Failed get HBase table for : " + hTableName);
        }
        long start = System.currentTimeMillis();
        String value = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        for (int i = 0; i < 1000000; i++) {
            putRow("cd", value, String.valueOf(System.currentTimeMillis() * 1000000 + i));
        }
        System.out.println("Time cost " + (System.currentTimeMillis() - start) + " ms");
        closeHTable();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Please enter two parameter for zookeeper[ip:port], hTable name");
        }
        TestHBase tester = new TestHBase(args[0], args[1]);
        tester.batchPut();
        tester.close();
    }
}