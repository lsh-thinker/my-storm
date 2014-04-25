package edu.sjtu.se.dclab.click;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RepeatVisitBolt extends BaseRichBolt {
	private static final long serialVersionUID = -59616745516629352L;

	private OutputCollector collector;
	
	private Jedis jedis;
	private String host;
	private int port;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		host = stormConf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(stormConf.get(Conf.REDIS_PORT_KEY).toString());
		connectToRedis();
	}
	
	private void connectToRedis(){
		jedis = new Jedis(host, port);
		jedis.connect();
	}
	
	public boolean isConnected(){
		if (jedis == null)
			return false;
		return jedis.isConnected();
	}

	@Override
	public void execute(Tuple tuple) {
		String ip = tuple.getStringByField(Fields.IP);
		String url = tuple.getStringByField(Fields.URL);
		String clientKey = tuple.getStringByField(Fields.CLIENT_KEY);
		String key = url + ":" + clientKey;
		String value = jedis.get(key);
		if (value == null){
			jedis.set(key, "visited");
			collector.emit(new Values(clientKey, url, Boolean.TRUE.toString()));
		}else{
			collector.emit(new Values(clientKey, url, Boolean.FALSE.toString()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new backtype.storm.tuple.Fields(Fields.CLIENT_KEY, Fields.URL, Fields.UNIQUE));
	}

}
