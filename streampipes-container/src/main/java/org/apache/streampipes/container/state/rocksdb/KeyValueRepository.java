package org.apache.streampipes.container.state.rocksdb;

import org.rocksdb.ColumnFamilyHandle;

public interface KeyValueRepository<K, V> {
    void save(K key, V value, ColumnFamilyHandle columnFamily);
    V find(K key, ColumnFamilyHandle columnFamily);
    void delete(K key, ColumnFamilyHandle columnFamily);
}
