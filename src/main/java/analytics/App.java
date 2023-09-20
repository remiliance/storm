package analytics;


import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.LocalCluster;
import org.apache.storm.tuple.Fields;

public class App 
{
    public static void main( String[] args )
    {
    	TopologyBuilder builder = new TopologyBuilder();
    	builder.setSpout("page-visits", new PageVisitSpout());
		builder.setBolt("visit-counts", new PageVisitBolt(), 2)
			.fieldsGrouping("page-visits", new Fields("userId"));
    	StormTopology topology = builder.createTopology();
    	LocalCluster cluster = new LocalCluster();
    	Config config = new Config();
    	cluster.submitTopology("analytics", config, topology);
    }
}
