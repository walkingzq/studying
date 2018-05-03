package consumer;

/**
 * Created by Zhao Qing on 2018/5/2.
 */
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleConsumer {
    public static void main(String args[]) {
        SampleConsumer example = new SampleConsumer();
//        long maxReads = Long.parseLong(args[0]);
        long maxReads = 100000;//最大测试读取信息数
//        String topic = args[1];
        String topic = "test";//topic名称
//        int partition = Integer.parseInt(args[2]);
        int partition = 0;//分区
        List<String> seeds = new ArrayList<String>();
        seeds.add("192.168.188.125");//broker list
//        seeds.add(args[3]);
//        int port = Integer.parseInt(args[4]);
        int port = 9092;//端口号
        try {
            example.run(maxReads, topic, partition, seeds, port);
        } catch (Exception e) {
            System.out.println("Oops:" + e);
            e.printStackTrace();
        }
    }

    private List<String> m_replicaBrokers = new ArrayList<String>();

    public SampleConsumer() {//构造函数
        m_replicaBrokers = new ArrayList<String>();
    }

    /**
     *
     * @param a_maxReads 最大读取数目
     * @param a_topic topic名称
     * @param a_partition 分区名称
     * @param a_seedBrokers broker信息
     * @param a_port broker端口
     * @throws Exception
     */
    public void run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws Exception {
        // find the meta data about the topic and partition we are interested in
        // 找到leader broker
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
        if (metadata == null) {//如果返回结果为空，打印异常并返回
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {//如果返回结果中没有leader信息，打印异常并返回
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();//获取broker leader的host
        String clientName = "Client_" + a_topic + "_" + a_partition;//构造消费者客户端名称

        //创建一个SimpleConsumer对象
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
        //设置offset：
        // 1.kafka.api.OffsetRequest.EarliestTime() --> 从最早的消息开始消费
        // 2.kafka.api.OffsetRequest.LatestTime() --> 从最近的消息开始消费
//        long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        long earliestOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        long latestOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
        System.out.println("offset取值范围: " + "[" + earliestOffset + "," + latestOffset + "]");
//        long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
        int numErrors = 0;
        long readOffset = earliestOffset + (latestOffset - earliestOffset) / 2;//人为设置起点 //TODO:暴力设置起点...
        System.out.println("起始offset: " + readOffset);
        while (a_maxReads > 0) {//循环，读取a_maxReads条数据
            if (consumer == null) {//如果consumer为空，则先初始化consumer
                consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
            }
            //读取数据
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    //在这里指定起始offset！！TODO
                    .addFetch(a_topic, a_partition, readOffset, 100000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);// Fetch a set of messages from a topic.

            //错误处理
            if (fetchResponse.hasError()) {
                numErrors++;//记录失败次数
                // Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;//如果失败次数大于5，则结束循环
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {//如果错误码为1（表示offset超出范围），则将起始offset设为最近的offset
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);//程序运行至此时，以上情形均不满足。可能是broker的问题，所以重新发现leadBroker
                continue;
            }
            numErrors = 0;//重置失败次数

            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {//MessageAndOffset类：Compute the offset of the next message in the log
                long currentOffset = messageAndOffset.offset();
//                System.out.println("currentOffset:" + currentOffset);
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();//获取下一条消息的offset
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
                numRead++;//记录读取数据条数
                a_maxReads--;//读取数据最大条数减一，决定循环结束的条件
            }

            if (numRead == 0) {//如果没有数据要读，休眠1000ms
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }//循环结束
        if (consumer != null) consumer.close();//如果SimpleConsumer对象不为空，则需要手动关闭
    }

    /**
     *
     * @param consumer 消费者
     * @param topic topic名称
     * @param partition 分区
     * @param whichTime offset
     * @param clientName 客户端名称
     * @return long offset
     */
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);//TopicAndPartition类：记录一个topic和partition
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();//new一个Map对象
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));//在map中添加元素，whichTime是起始offset，maxNumOffsets是offset获取最大数量
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);//
        //开始请求，Get a list of valid offsets (up to maxSize) before the given time，在这里maxSize为1
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {//错误处理
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);//拿到offset
//        System.out.println("offset:" + offsets[0]);//打印offset
        return offsets[0];//返回第一个offset，本例中只有一个offset
    }

    /**
     *
     * @param a_oldLeader 旧的broker leader host
     * @param a_topic topic
     * @param a_partition 分区
     * @param a_port broker端口
     * @return String 新的broker leader host
     * @throws Exception 找不到新的broker leader时抛出异常
     */
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);//
            //如果发生以下三种情况，则设定goToSleep为true
            // 1.查找结果为空
            // 2.查找结果中没有leader
            //3.新查找的leader与原leader相同且i为0（i为查找次数）
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {//如果goToSleep为true，线程进入休眠状态1000ms
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");//打印异常信息
        throw new Exception("Unable to find new leader after Broker failure. Exiting");//抛出异常
    }

    /**
     *遍历元数据信息，直到找到指定topic的指定partition的元数据信息，并返回该元数据信息
     * @param a_seedBrokers broker信息
     * @param a_port broker端口
     * @param a_topic topic名称
     * @param a_partition 分区名称
     * @return PartitionMetadata 指定topic的指定partition的元数据信息
     */
    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);//versionId:0,correlationId:0,clientId:"",topics:$topics
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);//SimpleConsumer.send()方法返回topics中所有topic的元数据信息，在这里topics中只有一个元素

                List<TopicMetadata> metaData = resp.topicsMetadata();//
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {//如果找到指定topic的指定partition，赋给returnMetaData并结束循环
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());//将该分区对应的所有broker添加至m_replicaBrokers列表中
            }
        }
        return returnMetaData;//返回returnMetaData
    }
}
