/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.javahelps.wisdom.core.operand;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class WisdomArray implements WisdomDataType<WisdomArray>, Iterable {

    private final List<Object> list;
    private final int size;

    protected WisdomArray(List<Object> list) {
        this.list = new ArrayList<>(list);
        this.size = this.list.size();
    }

    public static WisdomArray of(String... items) {
        return new WisdomArray(List.of(items));
    }

    public static WisdomArray of(int... items) {
        return new IntWisdomArray(items);
    }

    public static WisdomArray of(double... items) {
        return new DoubleWisdomArray(items);
    }

    public static WisdomArray of(List<Object> items) {
        return new WisdomArray(items);
    }

    public boolean contains(Comparable item) {
        if (item instanceof WisdomArray) {
            return this.list.containsAll(((WisdomArray) item).list);
        } else {
            return this.list.contains(item);
        }
    }

    public int size() {
        return this.list.size();
    }

    public <T> List<T> toList(Class<T> clz) {
        List<T> lst = new ArrayList<>(this.size);
        this.list.forEach(x -> lst.add((T) x));
        return lst;
    }

    public List<Object> toList() {
        return list;
    }

    public Object toArray() {
        return this.list.toArray();
    }

    public WisdomArray toIntArray() {
        int[] array = new int[this.size];
        for (int i = 0; i < this.size; i++) {
            array[i] = ((Number) this.list.get(i)).intValue();
        }
        return new IntWisdomArray(this.list, array);
    }

    public WisdomArray toLongArray() {
        long[] array = new long[this.size];
        for (int i = 0; i < this.size; i++) {
            array[i] = ((Number) this.list.get(i)).longValue();
        }
        return new LongWisdomArray(this.list, array);
    }

    public WisdomArray toDoubleArray() {
        double[] array = new double[this.size];
        for (int i = 0; i < this.size; i++) {
            array[i] = ((Number) this.list.get(i)).doubleValue();
        }
        return new DoubleWisdomArray(this.list, array);
    }

    public WisdomArray toFloatArray() {
        float[] array = new float[this.size];
        for (int i = 0; i < this.size; i++) {
            array[i] = ((Number) this.list.get(i)).floatValue();
        }
        return new FloatWisdomArray(this.list, array);
    }

    @Override
    public int compareTo(WisdomArray array) {
        if (this.equals(array)) {
            return 0;
        } else {
            return this.list.toString().compareTo(array.list.toString());
        }
    }

    @Override
    public int hashCode() {
        return this.list.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WisdomArray that = (WisdomArray) o;
        return Objects.equals(list, that.list);
    }

    @Override
    public String toString() {
        return this.list.toString();
    }

    @Override
    public Iterator iterator() {
        return this.list.iterator();
    }
}
