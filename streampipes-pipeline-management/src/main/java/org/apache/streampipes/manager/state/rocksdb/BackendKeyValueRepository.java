package org.apache.streampipes.manager.state.rocksdb;

import org.rocksdb.ColumnFamilyHandle;

public interface BackendKeyValueRepository<K, V> {
    void save(K key, V value, ColumnFamilyHandle cfHandle);
    V find(K key, ColumnFamilyHandle cfHandle);
    void delete(K key, ColumnFamilyHandle cfHandle);
}
