package com.hazelcast.merkletree;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Math.abs;

public class ArrayMerkleTree implements MerkleTree {
    // range_low(4) | range_high(4) | node_hash(4)
    private static final int NODE_SIZE = 4 + 4 + 4;
    private static final int OFFSET_RANGE_LOW = 0;
    private static final int OFFSET_RANGE_HIGH = 4;
    private static final int OFFSET_NODE_HASH = 8;
    private static final int MIN_DEPTH = 2;
    private static final int MAX_DEPTH = 27;

    private final ByteBuffer tree;
    private final int depth;
    private final int nodes;
    private final int leafs;
    private final int leafLevelOffset;
    private final double leafRangeStep;
    private final HashSet[] keysInLeaf;

    public ArrayMerkleTree(int depth) {
        if (depth < MIN_DEPTH || depth > MAX_DEPTH) {
            throw new IllegalArgumentException("Parameter depth " + depth + " is outside of the allowed range "
                    + MIN_DEPTH + "-" + MAX_DEPTH + ". ");
        }

        this.depth = depth;
        nodes = (1 << depth) - 1;
        leafs = (int) (((long) nodes + 1) / 2);
        leafLevelOffset = getLevelOffset(depth - 1);
        keysInLeaf = new HashSet[leafs];

        //                for (int i = 0; i < leafs; i++) {
        //                    keysInLeaf[i] = new HashSet();
        //                }

        int bufferSize = nodes * NODE_SIZE;
        tree = ByteBuffer.allocateDirect(bufferSize);

        long intRangeAsLong = abs((long) Integer.MIN_VALUE) + Integer.MAX_VALUE;
        leafRangeStep = ((double) intRangeAsLong / leafs);

        initTree();
    }

    private void initTree() {
        tree.putInt(0 + OFFSET_RANGE_LOW, Integer.MIN_VALUE);
        tree.putInt(0 + OFFSET_RANGE_HIGH, Integer.MAX_VALUE);

        for (int nodeIndex = 1; nodeIndex < nodes; nodeIndex++) {
            int nodeOffset = nodeIndex * NODE_SIZE;
            int parentIndex = getParentIndex(nodeIndex);
            int parentOffset = parentIndex * NODE_SIZE;
            int parentRangeLow = getNodeRangeLow(parentOffset);
            int parentRangeHigh = getNodeRangeHigh(parentOffset);

            boolean leftChild = nodeIndex % 2 == 1;
            int parentRangeMiddle = (int) (((long) parentRangeLow + parentRangeHigh) / 2);

            if (leftChild) {
                tree.putInt(nodeOffset + OFFSET_RANGE_LOW, parentRangeLow);
                tree.putInt(nodeOffset + OFFSET_RANGE_HIGH, parentRangeMiddle);
            } else {
                tree.putInt(nodeOffset + OFFSET_RANGE_LOW, parentRangeMiddle + 1);
                tree.putInt(nodeOffset + OFFSET_RANGE_HIGH, parentRangeHigh);
            }
        }
    }

    private void dumpTree() {
        int nodeIndex = 0;
        for (int i = 0; i < depth; i++) {
            System.out.print(i + ": ");
            for (int j = 0; j < 1 << i; j++) {
                int nodeOffset = nodeIndex * NODE_SIZE;
                int rangeLow = getNodeRangeLow(nodeOffset);
                int rangeHigh = getNodeRangeHigh(nodeOffset);
                int nodeHash = tree.getInt(nodeOffset + OFFSET_NODE_HASH);

                System.out.print("[" + nodeIndex + ":" + nodeOffset + "](" + rangeLow + "," + rangeHigh + "," + nodeHash + ") ");
                nodeIndex++;
            }
            System.out.println();
        }
    }

    private int getNodeRangeLow(int nodeOffset) {
        return tree.getInt(nodeOffset + OFFSET_RANGE_LOW);
    }

    private int getParentIndex(int nodeIndex) {
        return (nodeIndex - 1) >> 1;
    }

    private int getLeftChildIndex(int nodeIndex) {
        return 2 * nodeIndex + 1;
    }

    private int getLeftChildOffset(int nodeOffset) {
        return 2 * nodeOffset + NODE_SIZE;
    }

    private int getRightChildOffset(int nodeOffset) {
        return 2 * nodeOffset + 2 * NODE_SIZE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void update(Object key, Object value) {
        int keyHash = key.hashCode();
        int valueHash = value.hashCode();

        int leafOffset = findLeafOffset(keyHash);
        int leafCurrentHash = tree.get(leafOffset + OFFSET_NODE_HASH);
        int leafNewHash = hash(leafCurrentHash, valueHash);

        setNodeHash(leafOffset, leafNewHash);
        //        int leafOrder = (leafOffset - leafLevelOffset) / NODE_SIZE;
        //        keysInLeaf[leafOrder].add(key);
    }

    @Override
    public void recalculate() {
        for (int nodeOffset = leafLevelOffset - NODE_SIZE; nodeOffset >= 0; nodeOffset -= NODE_SIZE) {
            int leftChildOffset = getLeftChildOffset(nodeOffset);
            int rightChildOffset = getRightChildOffset(nodeOffset);

            int leftChildHash = getNodeHash(leftChildOffset);
            int rightChildHash = getNodeHash(rightChildOffset);

            int newNodeHash = hash(leftChildHash, rightChildHash);
            setNodeHash(nodeOffset, newNodeHash);
        }
    }

    private int findLeafOffset(int keyHash) {
        long hashDistanceFromLowest = ((long) keyHash) - Integer.MIN_VALUE;
        int leafDistanceFromLeftMost = (int) (hashDistanceFromLowest / leafRangeStep);
        int leafOffsetDistance = leafDistanceFromLeftMost * NODE_SIZE;
        int leafOffset = leafLevelOffset + leafOffsetDistance;

        if (getNodeRangeHigh(leafOffset) < keyHash) {
            leafOffset += NODE_SIZE;
        } else if (getNodeRangeLow(leafOffset) > keyHash) {
            leafOffset -= NODE_SIZE;
        }

        return leafOffset;
    }

    private int hash(int hash1, int hash2) {
        return hash1 + hash2;
    }

    private int getNodeRangeHigh(int nodeOffset) {
        return tree.getInt(nodeOffset + OFFSET_RANGE_HIGH);
    }

    private int getNodeHash(int nodeOffset) {
        return tree.getInt(nodeOffset + OFFSET_NODE_HASH);
    }

    private void setNodeHash(int nodeOffset, int hash) {
        tree.putInt(nodeOffset + OFFSET_NODE_HASH, hash);
    }

    public int depth() {
        return depth;
    }

    public int footprint() {
        return tree.capacity();
    }

    /**
     * Returns the offset of the leftmost node at the given level of the
     * tree
     *
     * @param level the level for which we find the index for
     * @return the offset of the level
     */
    private int getLevelOffset(int level) {
        if (level == 0) {
            return 0;
        }

        int nodeOffset = (2 << (level - 1)) - 1;
        return nodeOffset * NODE_SIZE;
    }

    public static void main(String[] args) {
        int depth = 8;
        ArrayMerkleTree merkleTree = new ArrayMerkleTree(depth);
        System.out.println("Depth: " + merkleTree.depth());
        System.out.println("Nodes: " + merkleTree.nodes);
        System.out.println("Memory footprint: " + merkleTree.footprint() + " bytes");

        int numberOfEntries = 10_000_000;
        int[] randomInts = new int[numberOfEntries];
        for (int i = 0; i < numberOfEntries; i++) {
            randomInts[i] = ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE);
        }

        long start = System.nanoTime();
        for (int i = 0; i < numberOfEntries; i++) {
            merkleTree.update(randomInts[i], randomInts[i]);
        }
        long end = System.nanoTime();

        long updateOverheadNanos = end - start;
        System.out.println("Adding " + numberOfEntries + " entries took " + (updateOverheadNanos / 1_000_000) + "ms");
        System.out.println("Average overhead time per entry: " + updateOverheadNanos / numberOfEntries + " ns");

        start = System.nanoTime();
        merkleTree.recalculate();
        end = System.nanoTime();

        double recalculateMillis = (end - start) / 1_000_000d;
        System.out.println("Recalculating the tree took: " + recalculateMillis + " ms");

        merkleTree.dumpTree();

    }
}
