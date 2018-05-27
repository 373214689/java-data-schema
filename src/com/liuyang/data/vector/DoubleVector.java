package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
	
	private List<Double> values;

	public DoubleVector(Schema schema, int initSize) {
		// 如果传入的Schema.type与Vector的type不匹配, 则抛出无效参数异常
		schemaCheck(schema.getType(), DEFAULT_VECTOR_TYPE);
		// 初始化
		this.schema = schema;
		this.values = Collections.synchronizedList(new ArrayList<Double>(initSize));
	}
	
	public DoubleVector(Schema schema) {
		this(schema, 1);
	}
	
	private DoubleVector() {

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
		values.add(value);
		return this;
	}
	
    @Override
	public final DoubleVector append(Object value) {
		if (value instanceof Double) {
			return append((double) value);
		}
		throw new UnsupportedOperationException();
	}
	
	@Override
	public DoubleVector append(PrimitveValue value) {
		if (value == null) throw new NullPointerException();
		schemaCheck(value.getType(), DEFAULT_VECTOR_TYPE);
	    values.add(value.getDouble());
		return this;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public DoubleVector clone() {
		DoubleVector newVector = new DoubleVector();
		newVector.schema = schema;
		newVector.values = (List<Double>) ((ArrayList<Double>) values).clone();
		return newVector;
	}
	
	@Override
	public DoubleVector fill(double value) {
		for(int i = 0; i < values.size(); i++) {
			values.set(i, value);
		}
		return this;
	}
	
	@Override
	public double getDouble(int index) {
		return values.get(index);
	}
	
	@Override
	public DoubleValue getValue(int index) {
		return new DoubleValue(values.get(index));
	}
	
	@Override
	public DoubleVector setValue(int index, double value) {
		values.set(index, value);
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
		return values.stream().map(DoubleValue::new);
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
