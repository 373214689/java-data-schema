package com.liuyang.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.liuyang.data.util.Schema;
import com.liuyang.data.vector.StructVector;

public class MatrixSet {
	
	private StructVector values;
	
	private Schema schema;
	
	private int limit = 20;
	
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
	    int size = size();
	    for(int i = 0; i < size; i++) {
	    	sets.add(tmp.getValueAsRow(i));
	    }

	    MatrixSet retval = new MatrixSet(tmp.schema(), sets.size());
	    sets.forEach(row -> {
	    	for(int i = 0; i < fields.size(); i++) {
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
				br.lines().map(mapper).forEach(e -> {if(bAppend) add(e);});
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
	
	public Schema schema() {
		return schema;
	}
	
	public String toString() {
		StringBuffer retval = new StringBuffer();
		System.out.println("values: size=" + size());
		retval.append("{");
		for(int i = 0; i < limit; i++) {
			
			retval.append(values.getValueAsRow(i).toString() + "\r\n");
		}
		retval.append("}");
		return retval.toString();
	}

	public int size() {
		return values.size();
	}
	
	public Stream<Row> stream() {
		return null;
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


}
