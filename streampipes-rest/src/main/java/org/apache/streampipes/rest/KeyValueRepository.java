package org.apache.streampipes.rest;

public interface KeyValueRepository<K, V> {
    void save(K key, V value);
    V find(K key);
    void delete(K key);
}