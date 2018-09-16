package org.apache.kafka.my;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 消息发送测试
 * 配置：config/server.properties --override num.partitions=1  --override replication-factor=2 --override log.segment.bytes=1024 --override log.index.size.max.bytes=1024
 */
public class ProducerTest {

    Producer producer = null;
    String topic = "test-topic-01";

    @Before
    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerr-test-01");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG,10000);//测试延时10s发送消息
        producer = new KafkaProducer<>(props);
    }

    @Test
    public void testSyncSend() {
        for(int messageNo=0;messageNo<10;messageNo++){
            String messageStr = "Message_" + messageNo;
            try {
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr)).get();
                System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testAsyncSend() {
        int messageNo = 1;
        long startTime = System.currentTimeMillis();
        String messageStr = "Message_" + messageNo;
        producer.send(new ProducerRecord<>(topic,
                messageNo,
                messageStr),
                new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        if (metadata != null) {
                            System.out.println(
                                    "message(" + messageNo + ", " + messageStr + ") sent to partition(" + metadata.partition() +
                                            "), " +
                                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
                        } else {
                            exception.printStackTrace();
                        }
                    }
                }
        );

        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

