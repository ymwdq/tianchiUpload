package io.openmessaging.demo;

import io.openmessaging.demo.YmSerial.YmMessageMeta3;
import io.openmessaging.demo.YmWriteModule.YmMessageStore3;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by YangMing on 2017/5/25.
 */
public class YmBucketCache6 {
    private HashMap<String, List<YmMessageMeta3>> cachedBucket;
    private final int MAX_BUCKET_SIZE = Config.MAX_CACHE_SIZE;
    private int currentSize;
    private YmMessageStore3 yms;

    public YmBucketCache6() {
        cachedBucket = new HashMap<>(Config.MESSAGE_CACHE_HASH_TABLE_SIZE);
        yms = YmMessageStore3.getInstance();
    }

    public synchronized void addMessage(YmMessageMeta3 message, String queueOrTopic) {
        currentSize += message.getTotalLength();
        if (cachedBucket.containsKey(queueOrTopic)) {
            cachedBucket.get(queueOrTopic).add(message);
        } else {
            List<YmMessageMeta3> tmp = new LinkedList<>();
            tmp.add(message);
            cachedBucket.put(queueOrTopic, tmp);
        }
        if (isFull()) {
            try {
                System.out.println("bucket full");
                yms.writeMessage(getCachedBucket(), currentSize);
//                MessageCounter.getInstance().countMessage(currentSize);
                releaseBucket();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isFull() {
        return currentSize >= MAX_BUCKET_SIZE;
    }


    public HashMap<String, List<YmMessageMeta3>> getCachedBucket() {
        return cachedBucket;
    }

    public void releaseBucket() {
        cachedBucket = new HashMap<>(Config.MESSAGE_CACHE_HASH_TABLE_SIZE);
        currentSize = 0;
    }

    public synchronized void flush() {
        try {
            System.out.println("flush");
            yms.writeMessage(cachedBucket, currentSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
