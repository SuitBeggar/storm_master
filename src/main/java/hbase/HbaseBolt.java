package hbase;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/2623:39
 * @Version:
 **/
public class HbaseBolt extends BaseBasicBolt{

    public static final String TableName = "m_table";
    //public static final String ColumnFamily = "rec_list";
    public static Configuration conf = HBaseConfiguration.create();
    protected static HTable table;

    public static void selectRowKey(String tablename, String rowKey) throws IOException {
        System.out.println("*****************");
        table = new HTable(conf, tablename);
        System.out.println("*****************");
        Get g = new Get(rowKey.getBytes());
        System.out.println("*****************");
        Result rs = table.get(g);
        System.out.println("*****************");

        System.out.println("==> " + new String(rs.getRow()));

        for (Cell kv : rs.rawCells()) {
            System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
            System.out.println("Column Family: " + new String(kv.getFamily()));
            System.out.println("Column       :" + new String(kv.getQualifier()));
            System.out.println("value        : " + new String(kv.getValue()));
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println(tuple.getString(0));
        conf.set("hbase.master", "192.168.231.10:60010");
        conf.set("hbase.zookeeper.quorum", "192.168.231.10,192.168.231.11,192.168.231.12");

        // TODO Auto-generated method stub
        try {
            System.out.println("[1]=============");
            selectRowKey(TableName, tuple.getString(0));
            System.out.println("[2]=============");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println("[3]=============");
            System.out.println(tuple);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
