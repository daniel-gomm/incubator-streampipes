package org.apache.streampipes.statedb.database;

import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PeStateDataBase implements StateDataBase {

    private String lastAdded;

    private final static String NAME = "state-db";
    private File dbDir;
    private RocksDB db;
    private final String columnFamily;
    private List<ColumnFamilyHandle> columnFamilyHandleList = null;

    public PeStateDataBase(String columnFamily) {
        this.columnFamily = columnFamily;
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
            db = RocksDB.open(options,
                    "/streampipes/rocks-db", cfDescriptors,
                    columnFamilyHandleList);

        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    void close(){
        //Close column family handles
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
            this.lastAdded = key;
        }catch(RocksDBException e){
            e.printStackTrace();
        }
    }


    @Override
    public String find(String key) {
        try{
            byte[] result = db.get(columnFamilyHandleList.get(1), key.getBytes());
            if(result == null) return null;
            return new String(result);
        }catch (RocksDBException e){
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public void delete(String key) {
        try{
            db.delete(columnFamilyHandleList.get(1), key.getBytes());
        } catch(RocksDBException e){
            e.printStackTrace();
        }
    }
}
