package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

import static io.openmessaging.config.MessageStoreConfig.SparseSize;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-07-04
 * Time: 下午3:28
 */
public class QueueIndex {

    private int[] index = new int[2100 / SparseSize];

    private int size = 0;

    public QueueIndex() {
    }

    public int getSize() {
        return size;
    }

    public void putIndex(int offset) {
        index[size++] = offset;
    }

    public int getIndex(int offset) {
        if (offset / SparseSize < size)
            return index[offset / SparseSize];
        else
            return -1;
    }
}