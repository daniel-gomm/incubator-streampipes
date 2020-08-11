package org.apache.streampipes.manager.state.rocksdb;

public interface BackendKeyValueRepository<K, V> {
    void save(K key, V value);
    V find(K key);
    void delete(K key);
}
