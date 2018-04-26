package com.javahelps.wisdom.core.event;

public class Index {

    private int[] indices;

    private Index(int... indices) {
        this.indices = indices;
    }

    public static Index of(int... indices) {
        return new Index(indices);
    }

    public static Index range(int start, int end) {
        final int size = end - start;
        int[] indices = new int[size];
        for (int i = 0; i < size; i++) {
            indices[i] = start + i;
        }
        return new Index(indices);
    }

    public int[] getIndices() {
        return indices;
    }
}
