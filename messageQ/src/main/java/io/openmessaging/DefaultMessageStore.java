package io.openmessaging;

import io.openmessaging.config.MessageStoreConfig;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.sun.corba.se.impl.util.RepositoryId.cache;
import static io.openmessaging.config.MessageStoreConfig.MAX_QUEUE_NUM;
import static io.openmessaging.config.MessageStoreConfig.SparseSize;
import static io.openmessaging.config.MessageStoreConfig.numCommitLog;


/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-06-26
 */

class DefaultMessageStore {

    private final MessageStoreConfig messageStoreConfig;

    private QueueIndex[] queueIndexTable = new QueueIndex[MAX_QUEUE_NUM];

    private DirectQueueCache[] queueMsgCache = new DirectQueueCache[MAX_QUEUE_NUM];
    private QueueCache[] queueCache = new QueueCache[MAX_QUEUE_NUM];

    private AtomicBoolean[] queueLock = new AtomicBoolean[MAX_QUEUE_NUM];

    private final CommitLogLite[] commitLogList = new CommitLogLite[numCommitLog];

    private boolean[] flushComplete = new boolean[MAX_QUEUE_NUM];

    private final AtomicBoolean consumeStart = new AtomicBoolean(false);

    private Lock[] bufferLock = new ReentrantLock[MAX_QUEUE_NUM];

    DefaultMessageStore(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;

        for (int i = 0; i < numCommitLog; i++)
            commitLogList[i] = (new CommitLogLite(messageStoreConfig.getFileSizeCommitLog(), getMessageStoreConfig().getStorePathCommitLog()));

        for (int topicId = 0; topicId < MAX_QUEUE_NUM; topicId++) {
            queueLock[topicId] = new AtomicBoolean(false);
            queueMsgCache[topicId] = new DirectQueueCache();
            queueIndexTable[topicId] = new QueueIndex();
            queueCache[topicId] = new QueueCache();
            bufferLock[topicId] = new ReentrantLock(false);
        }
    }

    private CommitLogLite getCommitLog(int index) {
        return commitLogList[index % numCommitLog];
    }

    void putMessage(int topicId, byte[] msg) {
        DirectQueueCache cache = queueMsgCache[topicId];
        int size = cache.addMessage(msg);
        if (size == SparseSize) {
            int offset = getCommitLog(topicId).putMessage(cache.getByteBuffer());
            queueIndexTable[topicId].putIndex(offset);
            cache.clear();
        }
    }

    private void flushAll() {
        for (int i = 0; i < MAX_QUEUE_NUM; i++) {
            flushCache(i);
            flushComplete[i] = true;
        }
    }

    private void flushCache(int topicId) {
        DirectQueueCache cache = queueMsgCache[topicId];
        int size = cache.getSize();
        if (size == 0) return;
//        if (size < SparseSize) cache.putTerminator();
//        int offset = getCommitLog(topicId).putMessage(cache.getByteBuffer());
        queueCache[topicId].getMsgList().addAll(cache.getMessage(0, size));
        cache.clear();
    }

    List<byte[]> getMessage(int topicId, int offset, int maxMsgNums) {

        if (consumeStart.compareAndSet(false, true)) {
            flushAll();
        } else {
            while (!flushComplete[topicId]) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        int off = offset;
        int nums = maxMsgNums;
        QueueIndex index = queueIndexTable[topicId];
        CommitLogLite commitLog = getCommitLog(topicId);

        List<byte[]> msgList = new ArrayList<>(maxMsgNums);

        /*消费开始*/
        if (offset == 0) queueLock[topicId].set(true);


        if (!queueLock[topicId].get()) {
            bufferLock[topicId].lock();

            try {
                DirectQueueCache cache = queueMsgCache[topicId];
                while (nums > 0 && index.getIndex(off) != -1) {
                    int start = off % SparseSize;
                    int end = Math.min(start + nums, SparseSize);
                    int phyOffset = index.getIndex(off);
                    try {
                        commitLog.getMessage(phyOffset, cache, start, end);
                        msgList.addAll(cache.getMessage(start, end));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    nums -= (end - start);
                    off += (end - start);
                }
//                ((DirectBuffer) cache.getByteBuffer()).cleaner().clean();

            } finally {
                bufferLock[topicId].unlock();
            }
        } else {
            while (nums > 0 && index.getIndex(off) != -1) {
                int start = off % SparseSize;
                int end = Math.min(start + nums, SparseSize);
                try {
                    int phyOffset = index.getIndex(off);
                    msgList.addAll(commitLog.getMessage(phyOffset, start, end));
                } catch (Exception e) {
                    e.printStackTrace();
                }

                nums -= (end - start);
                off += (end - start);
            }
        }
        if (nums > 0) {
            msgList.addAll(queueCache[topicId].getMsgList(off % SparseSize,
                    Math.min(off % SparseSize + nums, queueCache[topicId].size())));
        }
        return msgList;
    }

    private MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }
}
