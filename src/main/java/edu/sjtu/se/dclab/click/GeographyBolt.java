package edu.sjtu.se.dclab.click;

import java.util.Map;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeographyBolt extends BaseRichBolt {
	private static final long serialVersionUID = -934583913124844038L;
	
	private IPResolver resolver ;
	private OutputCollector collector;
	
	public GeographyBolt(HttpIPResolver resolver){
		this.resolver = resolver;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String ip = input.getStringByField(Fields.IP);
		JSONObject json = resolver.resolveIP(ip);
		String city = (String)json.get(Fields.CITY);
		String country = (String)json.get(Fields.COUNTRY_NAME);
		collector.emit(new Values(country,city));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new backtype.storm.tuple.Fields(Fields.COUNTRY, Fields.CITY));
	}

}
