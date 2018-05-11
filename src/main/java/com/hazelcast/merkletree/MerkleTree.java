package com.hazelcast.merkletree;

public interface MerkleTree {
    void update(Object key, Object value);

    void recalculate();

    int depth();

    int footprint();
}
