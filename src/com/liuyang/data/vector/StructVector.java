package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.liuyang.data.Row;
import com.liuyang.data.udf.UDF;
import com.liuyang.data.util.BinaryValue;
import com.liuyang.data.util.BooleanValue;
import com.liuyang.data.util.FloatValue;
import com.liuyang.data.util.IntValue;
import com.liuyang.data.util.LongValue;
import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.ShortValue;
import com.liuyang.data.util.TextValue;
import com.liuyang.data.util.Schema.Type;

/**
 * Struct Vector Class
 * 
 * @author liuyang
 * @version 1.0.0
 *
 */
public final class StructVector extends PrimitveVector {
	/** vector type */
	private final static Type DEFAULT_VECTOR_TYPE = Type.STRUCT;
	
	private Schema schema;
	
	private List<PrimitveVector> values;
	
	private StructVector() {
		
	}

	public StructVector(Schema schema, int initialCapacity) {
		this.schema = schema;
		this.values = new ArrayList<PrimitveVector>();
		for (Schema children: schema.getChildren()) {
			switch(children.getType()) {
			case BINARY:
				append(new BinaryVector(children, initialCapacity)); break;
			case DOUBLE:
				append(new DoubleVector(children, initialCapacity)); break;
			case FLOAT:
				append(new FloatVector(children, initialCapacity)); break;
			case INT:
			case INTEGER:
				append(new IntVector(children, initialCapacity)); break;
			case BIGINT:
			case LONG:
				append(new LongVector(children, initialCapacity)); break;
			case SHORT:
			case SMALLINT:
				append(new ShortVector(children, initialCapacity)); break;
			case STRING:
			default:
				append(new TextVector(children, initialCapacity)); break;
			}
		}
		System.out.println("StructVector: size=" + values.size());
	}
	
	public StructVector(Schema schema) {
		this(schema, 1);
	}
	
	@Override
	public StructVector append(PrimitveVector value) {
		values.add(value);
		return this;
	}
	
	/**
	 * 
	 * @param row
	 * @return
	 */
	public final StructVector appendRow(Row row) {
		if (row.schema() != schema) return this;
		for(int i = 0; i < values.size(); i++) {
			values.get(i).append(row.get(i));
		}
		return this;
	}
	
	public Row createRow() {
		Row retval = new Row(schema);
		for(int i = 0; i < values.size(); i++) {
			PrimitveValue value = null;
			switch (schema.getType(i)) {
			case BINARY:
				value = new BinaryValue() ; break;
			case DOUBLE:
				value = new BooleanValue(); break;
			case FLOAT:
				value = new FloatValue(); break;
			case INT:
			case INTEGER:
				value = new IntValue(); break;
			case BIGINT:
			case LONG:
				value = new LongValue(); break;
			case SHORT:
			case SMALLINT:
				value = new ShortValue(); break;
			case STRING:
			default:
				value = new TextValue(); break;
			}
			values.get(i).append(value);
			retval.set(i, value);
		}
		return retval;
	}
	
	@Override
	public boolean equals(Object other) {
		return other == this;
	}
	
	public StructVector distinct(String... fieldNames) {
		StructVector tmp = select(fieldNames);
		List<String> fields = tmp.schema.getFieldNames();
		HashSet<Row> sets = new HashSet<Row>();
	    int size = size();
	    for(int i = 0; i < size; i++) {
	    	sets.add(tmp.getValueAsRow(i));
	    }

	    
	    StructVector retval = new StructVector(tmp.schema, sets.size());
	    sets.forEach(row -> {
	    	for(int i = 0; i < fields.size(); i++) {
	    		retval.values.get(i).append(row);
	    	}
	    });
		return retval;
	}
	
	
	
	/**
	 * Get the type of vector.
	 * @return result see {@link Schema.Type}
	 */
	public Type getType() {
		return DEFAULT_VECTOR_TYPE;
	}
	
	public PrimitveVector get(int index) {
		return values.get(index);
	}
	
	public PrimitveVector get(String fieldName) {
		return values.get(schema.lookup(fieldName));
	}
	
	public Row getValueAsRow(int index) {
		Row retval = new Row(schema);
		for(int i = 0; i < retval.size(); i++) {
			PrimitveVector vector = values.get(i);
			retval.set(i, vector.getValue(index));
		}
		return retval;
	}

	
	public int hashCode(int index) {
		int result = 0, size = size();
		for(int i = 0; i < size; i++) {
	          result = result * 31 + values.get(i).getValue(index).hashCode();
	    }
		return result;
	}
	
    /**
     * Check schema
     * @param index
     * @param types
     */
    private void schemaCheck(int index, Type... types) {
    	Type type = schema.getType(index);
    	if (Arrays.stream(types).filter(e -> e.equals(type)).count() <= 0)
    		throw new IllegalArgumentException(
    				String.format("index: %d, type: %s can not match the %s", 
    						index, 
    						type,  
    						String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
    @Override
    public Schema schema() {
    	return schema;
    }
	
	/*public StructVector setBinary(int index, int offset, byte[] value) {
		schemaCheck(index, Type.BINARY);
		((BinaryVector) values).setValue(index, value);
		return this;
	}
	
	public StructVector setString(int index, int offset, String value) {
		schemaCheck(index, Type.STRING);
		((TextVector) values).setValue(index, value);
		return this;
	}*/

	
	public int size() {
		return values.stream().mapToInt(PrimitveVector::size).max().getAsInt();
	}
	
	public StructVector select(String... fieldNames) {
    	if (fieldNames == null) return this;
    	if (fieldNames.length == 0) return this;
    	if ("*".equals(fieldNames[0])) return this;
    	StructVector newVector = new StructVector();
    	newVector.schema = schema.select(fieldNames);
    	newVector.values = new ArrayList<PrimitveVector>();
    	final List<String> fields = newVector.schema.getFieldNames();
    	UDF.range(0, fields.size()).forEach(i -> {
    		newVector.values.add(get(fields.get(i)));
    	});
    	return newVector;
    	
	}
	
	public String toString() {
		int size = size();
		StringBuffer retval = new StringBuffer();
		retval.append("{");
		for(int i = 0; i < size; i++) {
			retval.append(getValueAsRow(i).toString() + "\r\n");
		}
		retval.append("}");
		return retval.toString();
	}
	
	@Override
	public void writeValue(OutputStream o) throws IOException {
		stream().forEach(v -> {
			try {
				v.writeValue(o);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});

	}

}
