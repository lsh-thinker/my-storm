package edu.sjtu.se.dclab.click;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeoStatsBolt extends BaseRichBolt {
	private static final long serialVersionUID = -140196303133234582L;
	
	private OutputCollector collector;
	private Map<String, CountryStats> stats = new HashMap<String, CountryStats>();

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String city = input.getStringByField(Fields.CITY);
		String country = input.getStringByField(Fields.COUNTRY);
		if (!stats.containsKey(country)){
			stats.put(country, new CountryStats(country));
		}
		stats.get(country).cityFound(city);
		collector.emit(new Values(country, stats.get(country).getCountryTotal(), 
				city, stats.get(country).getCityTotal(city)));
		System.out.println("=====================================================================");
		System.out.println("New Visitor Country:" + country + stats.get(country).getCountryTotal());
		System.out.println("City:" + city + stats.get(country).getCityTotal(city));
		System.out.println("---------------------------------------------------------------------");
		printStats();
		System.out.println("=====================================================================");
	}
	
	public void printStats(){
		for (Entry<String, CountryStats> entry : stats.entrySet()){
			System.out.println("Country: [" + entry.getKey() + "]");
			entry.getValue().printStats();
			System.out.println("-------------------------------------");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new backtype.storm.tuple.Fields(
				Fields.COUNTRY, Fields.COUNTRY_TOTAL, Fields.CITY, Fields.CITY_TOTAL));
	}

}
