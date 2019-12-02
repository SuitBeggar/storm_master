package wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/2422:12
 * @Version:
 **/
public class SplitSentenceBolt extends BaseRichBolt{

    OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        System.out.println("sentence: " + sentence);
        for (String word : sentence.split(" ")) {
            outputCollector.emit("split_stream", new Values(word));
            //_collector.emit(input, new Values(word));
            // TODO
//			outputCollector.ack(input);
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("split_stream", new Fields("word"));
    }
}
