package com.liuyang.data.udf;

import java.util.ArrayList;
import java.util.List;

public class UDF {
	/**
	 * Create an range from start to end.
	 * @param start
	 * @param end
	 * @return list contains value from start to end
	 */
	public static List<Integer> range(int start, int end) {
		if (end <= start) 
			throw new IllegalArgumentException("the start " + start + " must less than end " + end + ".");
		if (start < 0) 
			throw new IllegalArgumentException("the start can not be set 0.");
		if (end < 0) 
			throw new IllegalArgumentException("the end can not be set 0.");
		List<Integer> retval = new ArrayList<Integer>(end - start);
		for(int i = start; i < end; i++) {
		    retval.add(i);
		}
		return retval;
	}
}
