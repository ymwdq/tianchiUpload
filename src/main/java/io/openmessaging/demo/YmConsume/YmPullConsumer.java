package io.openmessaging.demo.YmConsume;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.ClientOMSException;
import io.openmessaging.demo.DefaultBytesMessage;

import java.util.*;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmPullConsumer implements PullConsumer{
    private KeyValue properties;
    private String queue;
    private Set<String> topics = new HashSet<>();
    private List<DefaultBytesMessage> msgList;
    private boolean isFinish;
    private boolean isFirst;
    private int offset;
    private YmMessageDistributor distributor = YmMessageDistributor.getInstance();

    public YmPullConsumer(KeyValue properties) {
        this.properties = properties;
        isFinish = false;
        isFirst = true;
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {
        if (isFinish) return null;
        else if (isFirst) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            isFirst = false;
            System.out.println(Thread.currentThread().getName() + "注册完毕");
            distributor.query(this);
            return this.poll();
        }
        else if (isListConsumeOver()) {
            distributor.consumeOver();
            distributor.query(this);
            if (msgList != null) {
                initOffset();
                System.out.println("consume success");
                return msgList.get(offset++);
            } else {
                // 请求填充，如果还为空,自锁
                return this.poll();
            }
        } else {
            System.out.println("consumer success");
            return msgList.get(offset++);
        }
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {

        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        this.topics.addAll(topics);
        distributor.submitConsumer(this, queueName, topics);
    }

    public boolean isListConsumeOver() {
        // 一开始没有初始化时，认为当前list消费完毕
        if (msgList == null) return true;
        return offset >= msgList.size();
    }

    public void setFinish(boolean isFinish) {
        this.isFinish = isFinish;
    }

    public void initOffset() {
        offset = 0;
    }

    public void setMsgList(List<DefaultBytesMessage> msgList) {
        this.msgList = msgList;
    }

    public boolean isEmpty() {
        return isListConsumeOver();
    }
}
