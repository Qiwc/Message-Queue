package io.openmessaging;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import io.openmessaging.utils.LibC;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.config.MessageStoreConfig.MESSAGE_SIZE;
import static io.openmessaging.config.MessageStoreConfig.QUEUE_CACHE_SIZE;


/**
 * Created by IntelliJ IDEA.
 * User: yangyuming
 * Date: 2018/7/6
 * Time: 下午8:15
 */
class DirectQueueCache {

    private ByteBuffer byteBuffer;

    private byte size = 0;

    private int offset = -1;

    DirectQueueCache() {
        this.byteBuffer = ByteBuffer.allocateDirect(QUEUE_CACHE_SIZE);
//        final long address = ((DirectBuffer) byteBuffer).address();
//        Pointer pointer = new Pointer(address);
//        LibC.INSTANCE.mlock(pointer, new NativeLong(QUEUE_CACHE_SIZE));
    }

    public void munlock() {
        final long address = ((DirectBuffer) byteBuffer).address();
        Pointer pointer = new Pointer(address);
        LibC.INSTANCE.munlock(pointer, new NativeLong(QUEUE_CACHE_SIZE));
    }

    int addMessage(byte[] msg) {
        byteBuffer.put((byte) msg.length);
        byteBuffer.put(msg);
        byteBuffer.position(++size * MESSAGE_SIZE);
        return size;
    }

    ByteBuffer getByteBuffer() {
        /*limit设为当前position,position设为0*/
        return byteBuffer;
    }

    void putTerminator() {
        byteBuffer.put((byte) 0);
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public byte getSize() {
        return size;
    }

    void clear() {
        /*position设为0，limit设为capacity，回到写模式*/
        byteBuffer.clear();
        size = 0;
    }

    /*ReadQueueCache*/

    ArrayList<byte[]> getMessage(int start, int end) {

        ArrayList<byte[]> msgList = new ArrayList<>();

        for (int i = start; i < end; i++) {
            byteBuffer.position(i * MESSAGE_SIZE);
            byte size = byteBuffer.get();
            if (size == 0) break;
            byte[] msg = new byte[size];
            byteBuffer.get(msg, 0, size);
            msgList.add(msg);
        }

        return msgList;
    }

    public ByteBuffer getWriteBuffer() {
        byteBuffer.clear();
        return byteBuffer;
    }
}
