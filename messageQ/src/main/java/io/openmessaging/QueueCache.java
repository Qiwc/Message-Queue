package io.openmessaging;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-07-06
 * Time: 上午10:12
 */
public class QueueCache {
    private List<byte[]> msgList = new ArrayList<>();

    public QueueCache() {

    }

    public void addMessage(byte[] msg) {
        msgList.add(msg);
    }

    public List<byte[]> getMsgList() {
        return msgList;
    }

    public List<byte[]> getMsgList(int start, int end) {
        return msgList.subList(start, end);
    }

    public int size() {
        return msgList.size();
    }

    public void clear() {
        msgList.clear();
    }
}
