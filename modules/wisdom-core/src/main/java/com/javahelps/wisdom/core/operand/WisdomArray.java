package com.javahelps.wisdom.core.operand;

import java.util.List;
import java.util.Objects;

public class WisdomArray implements WisdomDataType<WisdomArray> {

    private final List<Comparable> list;

    public WisdomArray(List<Comparable> list) {
        this.list = list;
    }

    public static WisdomArray of(Comparable... items) {
        return new WisdomArray(List.of(items));
    }

    public boolean contains(Comparable item) {
        if (item instanceof WisdomArray) {
            return this.list.containsAll(((WisdomArray) item).list);
        } else {
            return this.list.contains(item);
        }
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
}
