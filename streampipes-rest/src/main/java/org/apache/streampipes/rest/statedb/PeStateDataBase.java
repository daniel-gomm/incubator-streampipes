package org.apache.streampipes.rest.statedb;

import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PeStateDataBase implements StateDataBase {

    private byte[] lastAdded;

    private final static String NAME = "state-db";
    private RocksDB db;
    private final String columnFamily;
    private List<ColumnFamilyHandle> columnFamilyHandleList = null;
    private String path;
    private Long memoryDepth = 5L;

    public PeStateDataBase(String columnFamily) {
        this.columnFamily = columnFamily;
        this.path = "/tmp/streampipes/rocks-db/" + columnFamily;
        initialize();
    }


    public String getLastAdded(){
        return find(lastAdded);
    }

    void initialize(){
        RocksDB.loadLibrary();
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor(this.columnFamily.getBytes(), cfOpts)
            );
            // a list which will hold the handles for the column families once the db is opened
            columnFamilyHandleList =
                    new ArrayList<>();

            final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .optimizeForSmallDb();
            File f = new File(this.path);
            if (!f.exists()){
                try {
                    Files.createDirectories(f.getParentFile().toPath());
                    Files.createDirectories(f.getAbsoluteFile().toPath());
                } catch(IOException ex){
                    ex.printStackTrace();
                }
            }
            db = RocksDB.open(options,
                    this.path, cfDescriptors,
                    columnFamilyHandleList);
            ReadOptions ro = new ReadOptions();
            RocksIterator iterator = db.newIterator(columnFamilyHandleList.get(1));
            iterator.seekToLast();
            if(iterator.isValid()){
                lastAdded = iterator.key();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    void seek(){
        RocksIterator iter = db.newIterator(columnFamilyHandleList.get(1));
        for(iter.seekToFirst(); iter.isValid(); iter.next()){
            System.out.println(new String(iter.key()) + ":\n" + new String(iter.value()));
        }
    }

    void close(){
        //Close column family handles
        trim();
        for (final ColumnFamilyHandle columnFamilyHandle :
                columnFamilyHandleList) {
            columnFamilyHandle.close();
        }
        //Close the database
        db.close();
    }


    @Override
    public void save(String key, String value) {
        try{
            db.put(columnFamilyHandleList.get(1), key.getBytes(), value.getBytes());
            this.lastAdded = key.getBytes();
        }catch(RocksDBException e){
            e.printStackTrace();
        }
    }


    @Override
    public String find(String key) {
        return find(key.getBytes());
    }

    public String find(byte[] key){
        try{
            byte[] result = db.get(columnFamilyHandleList.get(1), key);
            if(result == null) return null;
            return new String(result);
        }catch (RocksDBException e){
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public void delete(String key) {
        delete(key.getBytes());
    }

    public void delete(byte[] key){
        try{
            db.delete(columnFamilyHandleList.get(1), key);
        } catch(RocksDBException e){
            e.printStackTrace();
        }
    }

    public void trim(){
        RocksIterator iterator = db.newIterator(columnFamilyHandleList.get(1));
        long max_length = this.memoryDepth;

        for(iterator.seekToLast(); iterator.isValid(); iterator.prev()){
            if(max_length--<=0){
                byte[] currentKey = iterator.key();
                iterator.seekToFirst();
                try {
                    db.deleteRange(columnFamilyHandleList.get(1), iterator.key(), currentKey);
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
    }
}
