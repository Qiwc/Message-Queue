package io.openmessaging;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-06-26
 */
public interface MessageStore {
    /**
     * Launch this message store.
     *
     * @throws Exception if there is any error.
     */
    void start() throws Exception;

    /**
     * Shutdown this message store.
     */
    void shutdown();

    /**
     * Store a message into store.
     *
     * @param msg Message instance to store
     * @return result of store operation.
     */
    void putMessage(final String topic, final byte[] msg);

    /**
     * Query at most <code>maxMsgNums</code> messages belonging to <code>topic</code> at <code>queueId</code> starting
     * from given <code>offset</code>. Resulting messages will further be screened using provided message filter.
     *
     * @param topic Topic to query.
     * @param offset Logical offset to start from.
     * @param maxMsgNums Maximum count of messages to query.
     * @return Matched messages.
     */
    List<byte[]> getMessage(final String topic, final long offset, final long maxMsgNums);

    /**
     * Get maximum offset of the topic queue.
     *
     * @param topic Topic name.
     * @return Maximum offset at present.
     */
    long getMaxOffsetInQueue(final String topic);

    /**
     * Get the minimum offset of the topic queue.
     *
     * @param topic Topic name.
     * @return Minimum offset at present.
     */
    long getMinOffsetInQueue(final String topic);

    /**
     * Get the offset of the message in the commit log, which is also known as physical offset.
     *
     * @param topic Topic of the message to lookup.
     * @param consumeQueueOffset offset of consume queue.
     * @return physical offset.
     */
    long getCommitLogOffsetInQueue(final String topic, final long consumeQueueOffset);

    /**
     * Get the total number of the messages in the specified queue.
     *
     * @param topic Topic
     * @return total number.
     */
    long getMessageTotalInQueue(final String topic);
}
