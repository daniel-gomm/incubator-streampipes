package org.apache.streampipes.manager.state.rocksdb;

import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

public class BackendStateDatabase implements BackendKeyValueRepository<byte[], byte[]> {

    //class objects
    private static String path;
    private static RocksDB db;
    private static ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    //instance objects
    private final String columnFamily;
    private ColumnFamilyHandle columnFamilyHandle = null;
    private byte[] lastAdded;
    private Long retainedCheckpoints;

    public BackendStateDatabase(String columnFamily){
        this(columnFamily, new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction()
                .optimizeForSmallDb(), 20L);
    }

    public BackendStateDatabase(String columnFamily, ColumnFamilyOptions cfOpts, Long retainedCheckpoints) {
        this.retainedCheckpoints = retainedCheckpoints;
        this.columnFamily = columnFamily;
        if(path == null){
            path = "/tmp/streampipes/rocks-db/backend";
        }
        if(db == null){
            initialize();
        }
        try {
            this.columnFamilyHandle = db.createColumnFamily(new ColumnFamilyDescriptor(this.columnFamily.getBytes(), cfOpts));
            columnFamilyHandles.add(this.columnFamilyHandle);
            RocksIterator iterator = db.newIterator(this.columnFamilyHandle);
            iterator.seekToLast();
            if(iterator.isValid()){
                lastAdded = iterator.key();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    private void initialize(){
        RocksDB.loadLibrary();
        //Check if directory and file already exist, if not create them
        File f = new File(path);
        if (!f.exists()){
            try {
                Files.createDirectories(f.getParentFile().toPath());
                Files.createDirectories(f.getAbsoluteFile().toPath());
            } catch(IOException ex){
                ex.printStackTrace();
            }
        }
        //options for the DB
        final Options options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .optimizeForSmallDb();

        try {
            db = RocksDB.open(options, path);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public static void close(){
        //close open column family handles
        for(ColumnFamilyHandle cfh : columnFamilyHandles){
            cfh.close();
        }
        //close database and set to null to indicate that it has to be opened again
        db.close();
        db = null;
    }

    @Override
    public void save(byte[] key, byte[] value) {
        try{
            db.put(columnFamilyHandle, key, value);
            this.lastAdded = key;
            trim();
        }catch(RocksDBException e){
            e.printStackTrace();
        }
    }

    public void save(String key, String value){
        save(key.getBytes(), value.getBytes());
    }

    @Override
    public byte[] find(byte[] key) {
        try{
            byte[] result = db.get(columnFamilyHandle, key);
            if(result == null) return null;
            return result;
        }catch (RocksDBException e){
            e.printStackTrace();
        }
        return null;
    }

    public String find(String key) {
        return new String(find(key.getBytes()));
    }

    public String findLast(){
        return new String(find(this.lastAdded));
    }

    @Override
    public void delete(byte[] key) {
        try{
            db.delete(columnFamilyHandle, key);
        } catch(RocksDBException e){
            e.printStackTrace();
        }
    }

    public void delete(String key){ delete(key.getBytes());}

    public void closeFamily(){
        this.columnFamilyHandle.close();
        //If all column families have been closed, close the db
        columnFamilyHandles.remove(this.columnFamilyHandle);
        if(columnFamilyHandles.isEmpty()){
            close();
        }
    }


    public void trim() {
        RocksIterator iterator = db.newIterator(this.columnFamilyHandle);
        long max_length = this.retainedCheckpoints;

        for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
            if (max_length-- <= 0) {
                byte[] currentKey = iterator.key();
                iterator.seekToFirst();
                try {
                    db.deleteRange(this.columnFamilyHandle, iterator.key(), currentKey);
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
    }
}
