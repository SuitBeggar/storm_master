package hbase;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import kafka.PrinterBolt;
import storm.kafka.*;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/2522:22
 * @Version:
 **/
public class stormKafka {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String topic = "topic_test";
        String zkRoot = "/topic_test";
        String spoutId = "kafkaSpout";

        BrokerHosts brokerHosts = new ZkHosts("master:2181");

        SpoutConfig kafkaConf = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);

        kafkaConf.forceFromStart = true;

        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout",kafkaSpout);

        builder.setBolt("printer",new HbaseBolt()).shuffleGrouping("spout");

        Config config = new Config();
        config.setDebug(false);

        if(args!=null && args.length > 0) {
            config.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            config.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, builder.createTopology());

//            Thread.sleep(10000);

//            cluster.shutdown();
        }
    }
}
