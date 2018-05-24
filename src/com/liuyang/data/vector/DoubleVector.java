package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.liuyang.data.util.DoubleValue;
import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.Schema.Type;

public final class DoubleVector extends PrimitveVector {
	/** vector type */
	private final static Type DEFAULT_VECTOR_TYPE = Type.DOUBLE;
	
	private Schema schema;
	
	private List<DoubleValue> values;

	public DoubleVector(Schema schema, int initSize) {
		// 如果传入的Schema.type与Vector的type不匹配, 则抛出无效参数异常
		schemaCheck(schema.getType(), DEFAULT_VECTOR_TYPE);
		// 初始化
		this.schema = schema;
		this.values = new ArrayList<DoubleValue>(initSize);
	}
	
	public DoubleVector(Schema schema) {
		this(schema, 1);
	}
	
	@Override
	protected void finalize() {
		values.clear();
		schema = null;
		values = null;
	}
	
	/**
     * Check schema
     * @param index
     * @param types
     */
    private void schemaCheck(Type src, Type... types) {
    	if (Arrays.stream(types).filter(e -> e.equals(src)).count() <= 0)
    		throw new IllegalArgumentException(
    				String.format("Source schem type: %s can not match the DoubleVector type %s"
    						,src
    						,String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
	@Override
	public DoubleVector append(double value) {
		values.add(new DoubleValue(value));
		return this;
	}
	
	@Override
	public DoubleVector append(PrimitveValue value) {
		if (value == null) throw new NullPointerException();
		schemaCheck(value.getType(), DEFAULT_VECTOR_TYPE);
	    values.add((DoubleValue) value);
		return this;
	}
	
	@Override
	public DoubleVector fill(double value) {
		for(int i = 0; i < values.size(); i++) {
			values.set(i, new DoubleValue(value));
		}
		return this;
	}
	
	@Override
	public double getDouble(int index) {
		return values.get(index).getDouble();
	}
	
	@Override
	public DoubleValue getValue(int index) {
		return values.get(index);
	}
	
	@Override
	public DoubleVector setValue(int index, double value) {
		values.set(index, new DoubleValue(value));
		return this;
	}
	
	@Override
	public Type getType() {
		return DEFAULT_VECTOR_TYPE;
	}

	@Override
	public int size() {
		return values.size();
	}
	
	@Override
	public Schema schema() {
		return schema;
	}
	
	@Override
	public Stream<DoubleValue> stream() {
		return values.stream();
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
