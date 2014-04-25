package edu.sjtu.se.dclab.click;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CountryStats {
	private int countryTotal = 0;
	private static final int COUNT_INDEX = 0;
	private static final int PERCENTAGE_INDEX = 1;
	
	
	private String countryName;


	public CountryStats(String countryName) {
		super();
		this.countryName = countryName;
	}
	
	private Map<String, List<Integer>> cityStats = new HashMap<String, List<Integer>>();
	
	public void cityFound(String city){
		++countryTotal;
		if(cityStats.containsKey(city)){
			int tmp = cityStats.get(city).get(COUNT_INDEX);
			cityStats.get(city).set(COUNT_INDEX, tmp + 1);
		}else{
			List<Integer> count = new ArrayList<Integer>();
			count.add(1);
			count.add(0);
			cityStats.put(city, count);
		}
		
		double percent = ((double)(cityStats.get(city).get(COUNT_INDEX)))/((double)countryTotal);
		cityStats.get(city).set(PERCENTAGE_INDEX, (int)(percent*100));
	}

	public int getCountryTotal() {
		return countryTotal;
	}
	
	public void printStats(){
		for (Entry<String, List<Integer>> en: cityStats.entrySet()){
			System.out.print("City is :" + en.getKey());
			System.out.print("\tVisitors count:" + en.getValue().get(COUNT_INDEX));
			System.out.print("\tPercentage count:" + en.getValue().get(PERCENTAGE_INDEX) + "%\n");
		}
		System.out.println("");
	}
	
	public int getCityTotal(String city){
		return cityStats.get(city).get(COUNT_INDEX);
	}
	
	public String toString(){
		return "Total Count for " + countryName + " is " + Integer.toString(countryTotal) + "\n"
                +  "Cities:  " + cityStats.toString();
	}
}







