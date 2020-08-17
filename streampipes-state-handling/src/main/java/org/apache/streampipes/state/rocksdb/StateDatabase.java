package org.apache.streampipes.state.rocksdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public enum StateDatabase implements KeyValueRepository<byte[], byte[]> {
    DATABASE;

    private final Logger LOG = LoggerFactory.getLogger(StateDatabase.class);
    private String path;
    private RocksDB db;
    private ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    private HashSet<byte[]> columnFamiliesBytes = new HashSet<>();
    private ArrayList<RocksObject> rocksObjects = new ArrayList<>();



    public ColumnFamilyHandle registerColumnFamily(String elementId) {
        LOG.info("Registering Pipeline Element: " + elementId);
        //TODO Remove print statement
        System.out.println(elementId);
        if(path == null){
            LOG.error("Could not register Column Family, path has not been initialized.");
            return null;
        }
        if(db == null && path != null){
            initialize(path);
        }
        try {
            for(ColumnFamilyHandle cfHandle : this.columnFamilyHandles){
                System.out.println(new String(cfHandle.getName()));
                if(Arrays.equals(cfHandle.getName(), elementId.getBytes())){
                    return cfHandle;
                }
            }
            ColumnFamilyHandle cfHandle = db.createColumnFamily(new
                    ColumnFamilyDescriptor(elementId.getBytes(), new ColumnFamilyOptions()
                    .optimizeUniversalStyleCompaction()
                    .optimizeForSmallDb()));
            this.columnFamilyHandles.add(cfHandle);
            this.columnFamiliesBytes.add(elementId.getBytes());
            return cfHandle;
        } catch (RocksDBException e) {
            LOG.error("Could not register Pipeline Element " + elementId, e);
            e.printStackTrace();
        }
        return null;
    }


    public void initialize(String path){
        this.path = path;
        LOG.info("Initializing Database");
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

        ArrayList<ColumnFamilyDescriptor> cfDesc = new ArrayList<>();
        try{
            Options options = new Options();
            ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                    .optimizeForSmallDb()
                    .optimizeUniversalStyleCompaction();
            this.rocksObjects.add(options);
            this.columnFamiliesBytes.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            this.columnFamiliesBytes.addAll(RocksDB.listColumnFamilies(options, this.path));
            for(byte[] colFam : this.columnFamiliesBytes){
                cfDesc.add(new ColumnFamilyDescriptor(colFam, cfOpts));
                LOG.info("Opening column family " + new String(colFam));
            }
        }catch (RocksDBException e){
            e.printStackTrace();
        }


        try {
            DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .optimizeForSmallDb();
            this.rocksObjects.add(options);
            db = RocksDB.open(options, path, cfDesc, this.columnFamilyHandles);
            for(ColumnFamilyHandle cfHandle : this.columnFamilyHandles){
                new PipelineElementDatabase(new String(cfHandle.getName()), cfHandle);
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        LOG.info("Closing Database.");
        //close open column family handles
        for(ColumnFamilyHandle cfh : this.columnFamilyHandles){
            cfh.close();
        }
        this.columnFamilyHandles = new ArrayList<>();
        for(RocksObject ro : this.rocksObjects){
            ro.close();
        }
        this.rocksObjects = new ArrayList<>();
        //close database and set to null to indicate that it has to be opened again
        db.close();
        if(this.db == null)
            return;
        db = null;
    }

    @Override
    public void save(byte[] key, byte[] value, ColumnFamilyHandle columnFamily) {
        try{
            db.put(columnFamily, key, value);
        }catch(RocksDBException e){
            LOG.error("Could not save to key " + new String(key), e);
            e.printStackTrace();
        }
    }

    public void save(String key, String value, ColumnFamilyHandle columnFamily){
        save(key.getBytes(), value.getBytes(), columnFamily);
    }

    @Override
    public byte[] find(byte[] key, ColumnFamilyHandle columnFamily) {
        try{
            byte[] result = db.get(columnFamily, key);
            return result;
        }catch (RocksDBException e){
            LOG.error("Could not find key " + new String(key), e);
            e.printStackTrace();
        }
        return null;
    }

    public String find(String key, ColumnFamilyHandle columnFamily) {
        return new String(find(key.getBytes(), columnFamily));
    }

    public byte[] findLast(ColumnFamilyHandle columnFamily) {
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            iterator.seekToLast();
            if (iterator.isValid()) {
                return iterator.key();
            }
        }
        return null;
    }

    @Override
    public void delete(byte[] key, ColumnFamilyHandle columnFamily) {
        try{
            db.delete(columnFamily, key);
        } catch(RocksDBException e){
            LOG.error("Could not delete key " + new String(key), e);
            e.printStackTrace();
        }
    }

    public void delete(String key, ColumnFamilyHandle columnFamily){ delete(key.getBytes(), columnFamily);}

    public void deleteFamily(ColumnFamilyHandle columnFamily){
        try{
            this.columnFamiliesBytes.remove(columnFamily.getName());
            db.dropColumnFamily(columnFamily);
        }catch(RocksDBException e){
            LOG.error("Could not close column family.", e);
            e.printStackTrace();
        }
        columnFamily.close();
    }


    public void trim(ColumnFamilyHandle columnFamily, Long retainedCheckpoints) {
        RocksIterator iterator = db.newIterator(columnFamily);
        long max_length = retainedCheckpoints;

        for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
            if (max_length-- <= 0) {
                byte[] currentKey = iterator.key();
                iterator.seekToFirst();
                try {
                    db.deleteRange(columnFamily, iterator.key(), currentKey);
                } catch (RocksDBException e) {
                    LOG.error("Could not delete range. ", e);
                    e.printStackTrace();
                }
                break;
            }
        }
    }
}
