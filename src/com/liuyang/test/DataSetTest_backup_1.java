package com.liuyang.test;

import java.io.IOException;

import com.liuyang.data.DataSet;
import com.liuyang.data.Row;
import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;

public class DataSetTest2 {

	public static void main(String[] args) {
		double s = System.nanoTime();
		Schema schema = Schema.createStruct("test");
		schema.addField( Schema.createInteger("int"));
		schema.addField( Schema.createLong("lng"));
		schema.addField( Schema.createString("str"));
		schema.addField( Schema.createDouble("dbl"));
		schema.addField( Schema.createDouble("ran"));
		
		
		DataSet ds = new DataSet(schema);
		
		for(int i = 0; i < 1500000; i++) {
			Row row = ds.createRow();
			row.setValue("int", i);
			row.setValue("lng", i + 0L);
			row.setValue("str", "a");
			row.setValue("dbl", i * 32.211);
			row.setValue("ran", Math.random() * 100);
		}
		System.out.println(ds.get(0).get(2).getType());
		System.out.println("write used: " + String.format("%.2fms", (System.nanoTime() - s) / 1000000));
		//DataSet ds1 = ds.select("int");
		s = System.nanoTime();
		System.out.println(ds.select("str", "dbl", "ran"));
		System.out.println("select used: " + String.format("%.2fms", (System.nanoTime() - s) / 1000000));
		s = System.nanoTime();
		System.out.println(ds.select("str"));
		System.out.println("select used: " + String.format("%.2fms", (System.nanoTime() - s) / 1000000));
		s = System.nanoTime();
		System.out.println(ds.distinct("str"));
		System.out.println("distinct used: " + String.format("%.2fms", (System.nanoTime() - s) / 1000000));
		
		System.out.println("sum: " + ds.stream().mapToDouble(e -> e.getDouble("dbl")).sum());
		System.out.println("sum: " + ds.stream().mapToDouble(e -> e.getDouble("ran")).sum());
		ds.stream().filter(e -> e.getInteger("int") > 90000).forEach(e -> {
			//System.out.println(e.select("str").getValues());
			for(PrimitveValue value: e.select("str").getValues()) {
				//System.out.println(value);
				try {
					value.writeValue(System.out);
					//System.out.println();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} finally {
					
				}
			}
		});
		System.out.println("test completed");
		
	}

}
