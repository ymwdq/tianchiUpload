package io.openmessaging.demo.YmConsume;

import io.openmessaging.PullConsumer;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.YmSerial.YmChunkParser2;
import io.openmessaging.demo.YmWriteModule.YmMessageReader3;

import java.util.*;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmMessageDistributor {
    private static YmMessageDistributor distributor = new YmMessageDistributor();
    private HashMap<String, List<DefaultBytesMessage>> msgTable;
    private HashMap<String, Set<PullConsumer>> needTable;
    private HashMap<String, Set<PullConsumer>> attachTable;
    private Set<YmPullConsumer> waitedConsumer;
    private Set<String> unusedMsgListNames;
    private YmMessageReader3 ymr;
    private YmChunkParser2 chunkParser2;
    private int consumeOverNum;

    private YmMessageDistributor() {
        waitedConsumer = new HashSet<>(ConsumeConfig.CONSUMER_NUM * 4);
        attachTable = new HashMap<>(ConsumeConfig.ATTACH_TABLE_SIZE);
        needTable = new HashMap<>(ConsumeConfig.ATTACH_TABLE_SIZE);
        unusedMsgListNames = new HashSet<>(ConsumeConfig.NAMES_TABLE_SIZE);
        ymr = YmMessageReader3.getInstance();
        chunkParser2 = new YmChunkParser2();
        new ReadChunkThread().start();
    }

    private void init() {
        unusedMsgListNames = new HashSet<>(ConsumeConfig.NAMES_TABLE_SIZE);
        for (String key : attachTable.keySet()) {
            unusedMsgListNames.add(key);
            for (PullConsumer consumer : attachTable.get(key)) {
                addQueueOrTopicToTable(key, (YmPullConsumer)consumer, needTable);
                waitedConsumer.add((YmPullConsumer)consumer);
            }
        }
        consumeOverNum = genConsumeNum();
    }

    public static YmMessageDistributor getInstance() {
        return distributor;
    }

    public synchronized void setTable(HashMap<String, List<DefaultBytesMessage>> msgTable) {
        this.msgTable = msgTable;
    }

    public synchronized void submitConsumer(YmPullConsumer consumer, String queue, Collection<String> topics) {
        addQueueOrTopicToTable(queue, consumer, needTable);
        for (String topic : topics) {
            addQueueOrTopicToTable(topic, consumer, needTable);
        }
        waitedConsumer.add(consumer);
    }

    private void addQueueOrTopicToTable(String queue, YmPullConsumer consumer, HashMap<String, Set<PullConsumer>> table) {
        if (table.containsKey(queue)) {
            table.get(queue).add(consumer);
        } else {
            HashSet<PullConsumer> consumers = new HashSet<>();
            consumers.add(consumer);
            table.put(queue, consumers);
        }
    }

    private synchronized void offer() {
        for (String unusedMsgList : unusedMsgListNames) {
            Set<PullConsumer> needConsumers = needTable.get(unusedMsgList);
            for (PullConsumer consumer : needConsumers) {
                if (waitedConsumer.contains(consumer)) {
                    ((YmPullConsumer)consumer).setMsgList(msgTable.get(unusedMsgList));
                }
            }
        }
    }

    private boolean isDistributedOver() {
        return consumeOverNum == 0 || msgTable == null;
    }

    private int genConsumeNum() {
        int cnt = 0;
        for (String key : msgTable.keySet()) {
            cnt += attachTable.get(key).size();
        }
        return cnt;
    }

    public synchronized void consumeOver() {
        consumeOverNum--;
    }

    public synchronized void query(YmPullConsumer consumer) {
        if (!isDistributedOver()) {
            offer();
            if (consumer.isEmpty()) {
                waitedConsumer.add(consumer);
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {

        }
    }

    class ReadChunkThread extends Thread {
        @Override
        public void run() {
            while (true) {
                if (isDistributedOver()) {
                    byte[] metaData = ymr.readDataChunk();
                    chunkParser2.setMetaData(metaData);
                    chunkParser2.readChunk();
                    System.out.println("read chunk over");
                    msgTable = chunkParser2.getTable();
                    System.out.println("set table over");
                }
            }
        }
    }
}
