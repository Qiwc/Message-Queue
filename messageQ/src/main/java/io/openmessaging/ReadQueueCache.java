package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.openmessaging.config.MessageStoreConfig.SparseSize;
import static io.openmessaging.config.MessageStoreConfig.QUEUE_CACHE_SIZE;


/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/7/7
 * Time: 下午4:04
 */
public class ReadQueueCache {

    private final AtomicInteger refCount = new AtomicInteger(0);

    private ByteBuffer byteBuffer;

//    private int offset = -1;

    private int index = 0;
    private int pos = 0;
    public ReadQueueCache(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getWriteBuffer() {
        index = 0;
        pos = 0;
        byteBuffer.clear();
        return byteBuffer;
    }

//    public int getOffset() {
//        return offset;
//    }
//
//    public void setOffset(int offset) {
//        this.offset = offset;
//    }

    ArrayList<byte[]> getMessage(int start, int end) {

        ArrayList<byte[]> msgList = new ArrayList<>();

        if (index != start) {
            index = 0;
            pos = 0;
        }
        byteBuffer.position(pos);
        byteBuffer.limit(QUEUE_CACHE_SIZE);

        byte size;

        while (index < end){
            /*读取消息长度*/
            size = byteBuffer.get();

            if (size == 0) break;

            /*读取消息体*/
            byte[] msg = new byte[size];
            byteBuffer.get(msg, 0, size);

            if (index >= start)
                msgList.add(msg);
            index++;
            pos += size + 1;
        }

        return msgList;
    }

}