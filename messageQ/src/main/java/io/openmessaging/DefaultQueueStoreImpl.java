package io.openmessaging;

import io.openmessaging.common.LoggerName;
import io.openmessaging.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class DefaultQueueStoreImpl extends QueueStore {

//    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MessageStoreConfig messageStoreConfig;

    private final DefaultMessageStore messageStore;

    private TopicIdGenerator idGenerator = new TopicIdGenerator();

    public DefaultQueueStoreImpl() {
        this.messageStoreConfig = new MessageStoreConfig();
        this.messageStore = new DefaultMessageStore(messageStoreConfig);
    }

    @Override
    void put(String queueName, byte[] message) {

        int queueId = idGenerator.getId(queueName);
        messageStore.putMessage(queueId, message);

//        messageStore.putMessage(TopicIdGenerator.getInstance().getId(queueName), message);
    }

    @Override
    Collection<byte[]> get(String queueName, long offset, long num) {
        int queueId = idGenerator.getId(queueName);
        return messageStore.getMessage(queueId, (int) offset, (int) num);
    }
}
