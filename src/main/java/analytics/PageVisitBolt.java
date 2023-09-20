package analytics;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PageVisitBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private HashMap<String, Integer> pageVisitCounts;
	private HashMap<Integer, Integer> userVisitCounts;
	private Integer totalVisitCount;


	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String url = input.getStringByField("url");
		Integer userId = input.getIntegerByField("userId");
		
		pageVisitCounts.putIfAbsent(url, 0);
		userVisitCounts.putIfAbsent(userId, 0);
		
		pageVisitCounts.put(url,  pageVisitCounts.getOrDefault(url,0) + 1);
		userVisitCounts.put(userId,  userVisitCounts.getOrDefault(url,0) + 1);
		totalVisitCount += 1;

		System.out.printf("received visit #%d from user %d (total:%d) to page %s (total: %d)\n",
				totalVisitCount, userId, userVisitCounts.get(userId), url, pageVisitCounts.get(url));
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		pageVisitCounts = new HashMap<String, Integer>();
		userVisitCounts = new HashMap<Integer, Integer>();
		totalVisitCount = 0;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
