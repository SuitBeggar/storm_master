package http;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.storm.http.HttpEntity;
import org.apache.storm.http.client.methods.CloseableHttpResponse;
import org.apache.storm.http.client.methods.HttpGet;
import org.apache.storm.http.impl.client.CloseableHttpClient;
import org.apache.storm.http.impl.client.HttpClients;
import org.apache.storm.http.util.EntityUtils;

import java.io.IOException;

/**
 * @description:
 * @Author:bella
 * @Date:2019/11/2623:36
 * @Version:
 **/
public class HttpBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println(tuple);
        String content = tuple.getString(0);

        CloseableHttpClient httpClient = HttpClients.createDefault();

        System.out.println(content);
        HttpGet httpGet = new HttpGet("http://192.168.231.10:8808/?content=" + content);
        try {
            CloseableHttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();

            System.out.println(response.getStatusLine());
            if (entity != null) {
                System.out.println("Response content length: " + entity.getContentLength());
                String responseString = new String(EntityUtils.toString(entity));
                responseString = new String(responseString.getBytes("ISO-8859-1"), "utf-8");
                System.out.println(responseString);

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
