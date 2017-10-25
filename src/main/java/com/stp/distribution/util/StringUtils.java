package com.stp.distribution.util;
/**
 * 
 * @author hhbhunter
 *
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

public class StringUtils {
	public static boolean isEmpty(String str){
		return str == null || str.isEmpty() || "{}".equals(str);
	}
	
	public static boolean isNotEmpty(String str){
		return !isEmpty(str);
	}
	
	public static String join(Object[] array, String delim) {
		boolean first = true;
		StringBuilder b = new StringBuilder();
		for (Object token : array) {
			if (first) {
				first = false;
			}
			else {
				b.append(delim);
			}
			b.append(token);
		}
		return b.toString();
	}
	
	public static String join(int[] array, String delim) {
		boolean first = true;
		StringBuilder b = new StringBuilder();
		for (int token : array) {
			if (first) {
				first = false;
			}
			else {
				b.append(delim);
			}
			b.append(token);
		}
		return b.toString();
	}
	
	public static boolean isNumeric(String str) {
		if(isEmpty(str)){
			return false;
		}
		if (!Character.isDigit(str.charAt(0)) && str.charAt(0) != '-'  ) {
			return false;
		}
		for (int i = str.length(); --i >= 1;) {
			if (!Character.isDigit(str.charAt(i)) ) 
				return false;
		}
		return true;
	}
	
	public static Map<String, String> getStrMapByJSON(String json) {
		Map<String, Object> resultObject = getStrObjMapByJSON(json);
		Map<String, String> result = new HashMap<String, String>();
		if(resultObject == null) return result;
		for(Map.Entry<String, Object> entry : resultObject.entrySet()) {
			result.put(entry.getKey(), String.valueOf(entry.getValue()));
		}
		return result;
	}
	
	public static Map<String, Object> getStrObjMapByJSON(String json) {
		Map<String, Object> result = JSON.parseObject(json, new TypeReference<Map<String, Object>>(){}.getType());
//		Map<String, Object> result = new Gson().fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
		return result == null?new HashMap<String, Object>():result;
	}
	public static Map<String, List<String>> getStrMapListByJSON(String json) {
		Map<String, List<String>> result = new HashMap<String, List<String>>();
		Map<String, Object> resultObject = getStrObjMapByJSON(json);
		for(Map.Entry<String, Object> entry : resultObject.entrySet()) {
			if(entry.getValue() instanceof List){
			result.put(entry.getKey(), (List<String>) entry.getValue());
			}
		}
		return result == null?new HashMap<String, List<String>>():result;
	}
	public static Map<String, Map<String,String>> getStrMapMapByJSON(String json) {
		Map<String, Map<String,String>> result = new HashMap<String, Map<String,String>>();
		Map<String, Object> resultObject = getStrObjMapByJSON(json);
		for(Map.Entry<String, Object> entry : resultObject.entrySet()) {
			if(entry.getValue() instanceof Map){
			result.put(entry.getKey(), ( Map<String,String>) entry.getValue());
			}
		}
		return result == null?new HashMap<String, Map<String,String>>():result;
	}
	
	public static List<String> getStrListByJSON(String json) {
//		return new Gson().fromJson(json, new TypeToken<List<String>>() {}.getType());
		return JSON.parseObject(json, new TypeReference<List<String>>(){});
	}
	
	public static <T> List<T> arrToList(T[] tArr) {
		
		List<T> tList = new ArrayList<T>(tArr.length);
		
		for (T pseudocode:tArr) {
			tList.add(pseudocode);
		}
		
		return tList;
	}
	
	public static String toJson(Object obj) {
		return JSON.toJSONString(obj);
	}
	
	public static void main(String[] args) {
//		String json = "{\"current\":\"current\"}";
		String json = "{current:current}";
//		String json = "{\"log\":[\"remote work 10.126.88.10 /opt/esearch/0/version-200/searcher15/\",\"ls: cannot access /opt/esearch/0/version-200/searcher15/: No such file or directory\"]}";
		Map<String, String> map = getStrMapByJSON(json);
		System.out.println(map);
	}
}
