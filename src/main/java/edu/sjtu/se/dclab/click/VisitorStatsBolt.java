package edu.sjtu.se.dclab.click;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VisitorStatsBolt extends BaseRichBolt {
	private static final long serialVersionUID = 5601334319966021491L;
	
	private OutputCollector collector;
	private int total = 0;
	private int uniqueCount = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		boolean unique = Boolean.parseBoolean(tuple.getStringByField(Fields.UNIQUE));
        total++;
        if(unique)uniqueCount++;
        collector.emit(new Values(total,uniqueCount));
        System.out.println("=====================================================================");
        System.out.println("Total Visistors :" + total);
        System.out.println("Unique Visistors :" + uniqueCount);
        System.out.println("=====================================================================");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new backtype.storm.tuple.Fields(Fields.TOTAL_COUNT,Fields.TOTAL_UNIQUE));
	}

}
