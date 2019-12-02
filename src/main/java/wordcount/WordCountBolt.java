package wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/2423:00
 * @Version:
 **/
public class WordCountBolt extends BaseRichBolt{
    OutputCollector outputCollector;
    public Map<String , Integer> countMap = new HashMap<String ,Integer>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        System.out.println("word: " + word);
        Integer count = this.countMap.get(word);
        if (null == count)
        {
            count = 0;
        }
        count++;
        this.countMap.put(word, count);

        Iterator<String> iter = this.countMap.keySet().iterator();
        while(iter.hasNext())
        {
            String next = iter.next();
            System.out.println(next + ":" + this.countMap.get(next));
        }

        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
