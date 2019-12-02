package wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/2421:21
 * @Version:
 **/
public class WordCountStart {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("1",new WordCountSpout());

        builder.setBolt("2",new SplitSentenceBolt(),1).shuffleGrouping("1","spout_stream");

        builder.setBolt("3",new WordCountBolt(),1).fieldsGrouping("2","split_stream",new Fields("word"));

        Config conf = new Config();

        conf.setDebug(false);


        if(args[0].equals("local")){ //本地模式
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordcount-demo-123", conf, builder.createTopology());
        }else { //集群模式
            try {
                StormSubmitter.submitTopology("wordcount-online", conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                System.out.println("[AlreadyAliveException] error:" + e);
            } catch (InvalidTopologyException e) {
                System.out.println("[InvalidTopologyException] error:" + e);
            }
        }
    }
}
