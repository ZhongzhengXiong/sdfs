package sdfs.namenode;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {
    private int capacity;

    LRULinkedHashMap() {
        super(16, 0.75f, true);
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return (size() > capacity);
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }


}
