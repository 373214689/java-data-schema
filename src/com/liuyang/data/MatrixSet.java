package com.liuyang.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import com.liuyang.data.udf.UDF;
import com.liuyang.data.util.IntValue;
import com.liuyang.data.util.PrimitveValue.Mode;
import com.liuyang.data.util.Schema;
import com.liuyang.data.vector.PrimitveVector;
import com.liuyang.data.vector.StructVector;

public class MatrixSet {
	
	private StructVector values;
	
	private Schema schema;
	
	private Map<Row, StructVector> partitions;
	
	private int limit = 20;
	
	private Row lastRow = null;
	
	private MatrixSet() {
		
	}
	
	public MatrixSet(Schema schema, int initialCapacity) {
		this.schema = schema;
		this.values = new StructVector(schema, initialCapacity);
	}
	
	public MatrixSet(Schema schema) {
	    this(schema, 1);
	}

	protected void finalize() {
		values = null;
		schema = null;
	}
	
	/*public final Row createRow() {
		return values.createRow();
	}*/
	
	public final Row createRow() {
		return values.createRow();
	}
	
	public final MatrixSet add(Row row) {
		values.appendRow(row);
		return this;
	}
	
	public MatrixSet distinct(String... fieldNames) {
		StructVector tmp = values.select(fieldNames);
		List<String> fields = tmp.schema().getFieldNames();
		HashSet<Row> sets = new HashSet<Row>();
	   /* int size = size();
	    for(int i = 0; i < size; i++) {
	    	sets.add(tmp.getValueAsRow(i));
	    }*/
	    UDF.range(0, size(), 1, false).mapToObj(tmp::getValueAsRow).forEach(sets::add);
	    MatrixSet retval = new MatrixSet(tmp.schema(), sets.size());
	    //System.out.println("debug schema " + tmp.schema());
	    sets.forEach(row -> {
	    	//x++;
	    	//System.out.println("debug row " +  x + " " + tmp.getValueAsRow(0) );
	    	for(int i = 0; i < fields.size(); i++) {
	    		//System.out.println("debug field " +  i + " " + row.get(i).getType());
	    		retval.values.get(i).append(row.get(i));
	    	}
	    });
		return retval;
	}
	
	public MatrixSet readTextFile(Function<? super String, ? extends Row> mapper, boolean bAppend, String... paths) {
    	if (paths == null) return this;
    	for(String path: paths) {
    		BufferedReader br;
    		try {
				br = new BufferedReader(new FileReader(path));
				br.lines().map(mapper).parallel().forEach(e -> {if(bAppend) add(e);});
				br.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				br = null;
			}
    	}
    	//trimToSize();
    	return this;
    }
	
	public MatrixSet readTextFile(Function<? super String, ? extends Row> mapper, String... paths) {
    	return readTextFile(mapper, paths);
    }
	
	public MatrixSet readTextFile(Function<? super String, ? extends Row> mapper, Reader reader) {
    	if (reader == null) return this;
		BufferedReader br;
		try {
			br = new BufferedReader(reader);
			br.lines().map(mapper).parallel().forEach(this::add);
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			br = null;
		}
    	//trimToSize();
    	return this;
	}
	
	public Schema schema() {
		return schema;
	}
	
	public String toString() {
		StringBuffer retval = new StringBuffer();
		//System.out.println("values: size=" + size());
		retval.append("{");
		/*for(int i = 0; i < limit; i++) {
			
			retval.append(values.getValueAsRow(i).toString() + "\r\n");
		}*/
	    values.rows().limit(limit).forEach(row -> {
	    	retval.append(row + "\r\n");
	    });
		retval.append("}");
		return retval.toString();
	}

	public int size() {
		return values.size();
	}
	
	public Stream<Row> stream() {
		return values.rows();
	}
	
    public MatrixSet select(String... fieldNames) {
    	if (fieldNames == null) return this;
    	if (fieldNames.length == 0) return this;
    	if ("*".equals(fieldNames[0])) return this;
    	MatrixSet retval = new MatrixSet();
    	retval.values = values.select(fieldNames);
    	retval.schema = values.schema();
    	return retval;
    }
    
    public MatrixSet groupby(String... fieldNames) {
    	IntValue counter = new IntValue(0);
    	partitions = new HashMap<Row, StructVector>();
    	HashSet<Row> tmpSet = new HashSet<Row>();
    	
    	UDF.range(0, size(), 1, false)
    	   .mapToObj(values::getValueAsRow)
    	   .map(e -> e.groupby(fieldNames)).limit(10000)
    	   .forEach(row -> {
			Row key = row.keys();
			Row value = row.values();
			if (partitions.containsKey(key)) {
				partitions.get(key).appendRow(value);
				//partitions.put(row.keys(), row);
			} else {
				StructVector newVector = new StructVector(value.schema());
				newVector.appendRow(value);
				partitions.put(key, newVector);
				counter.modifyValue(Mode.INCREASE, 1);
			}
			//System.out.println(key + " hash " + key.hashCode() + " has " + tmpSet.contains(key));
			//tmpSet.add(row.keys());
			
			//if (lastRow != null) {
				//System.out.println("row equals schema compare " + lastRow.schema() + " == " + row.schema() + " >> " + lastRow.schema().equals(row.schema()));
			//	System.out.println(lastRow + "==" + row + " >> " + lastRow.equals(row));
			//}
			//lastRow = row;
			
		});
		
		System.out.println(partitions);
		//System.out.println(tmpSet);
		//System.out.println(tmpSet.size() + " <> " + counter);
    	
		return this;
	}


}
