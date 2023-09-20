package analytics;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

public class PageVisitSpout extends BaseRichSpout {
	private static SpoutOutputCollector outputCollector;

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		String[] urls = {"http://example.com/index.html", "http://example.com/404.html", "http://example.com/subscribe.html"};
		Integer[] userIds = {1, 2, 3, 4, 5};
		
		String url = urls[ThreadLocalRandom.current().nextInt(urls.length)];
		Integer userId = userIds[ThreadLocalRandom.current().nextInt(userIds.length)];
		
		Values values = new Values(url, userId);
		outputCollector.emit(values, values);
		Utils.sleep(2000);
		

	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("url", "userId"));

	}

}
