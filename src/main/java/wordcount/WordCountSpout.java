package wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/2421:26
 * @Version:
 **/
public class WordCountSpout extends BaseRichSpout implements IRichSpout {
    SpoutOutputCollector outputCollector;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        outputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        // TODO Auto-generated method stub
        String [] words = new String[] {"how do you do", "you do what", "do you kown"};
        Random rand = new Random();
        String word = words[rand.nextInt(words.length)];
//		outputCollector.emit(new Values(word));

        //Object msgid = "1";
        Object msgid = rand.hashCode();
        System.out.println("msgid" + msgid.toString());

        outputCollector.emit("spout_stream", new Values(word), msgid);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream("spout_stream",new Fields("sentence"));

    }
}
