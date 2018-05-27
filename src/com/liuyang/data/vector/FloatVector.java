package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.liuyang.data.util.FloatValue;
import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.Schema.Type;

public final class FloatVector extends PrimitveVector {
	/** vector type : FLOAT*/
	private final static Type DEFAULT_VECTOR_TYPE = Type.FLOAT;
	
	private Schema schema;
	
	private List<Float> values;

	public FloatVector(Schema schema, int initSize) {
		// 如果传入的Schema.type与Vector的type不匹配, 则抛出无效参数异常
		schemaCheck(schema.getType(), DEFAULT_VECTOR_TYPE);
		// 初始化
		this.schema = schema;
		this.values = Collections.synchronizedList(new ArrayList<Float>(initSize));
	}
	
	public FloatVector(Schema schema) {
		this(schema, 1);
	}
	
	private FloatVector() {

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
    				String.format("Source schem type: %s can not match the FloatVector type %s"
    						,src
    						,String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
	@Override
	public FloatVector append(float value) {
		values.add(value);
		return this;
	}
	
    @Override
	public final FloatVector append(Object value) {
		if (value instanceof Float) {
			return append((float) value);
		}
		throw new UnsupportedOperationException();
	}
	
	@Override
	public FloatVector append(PrimitveValue value) {
		if (value == null) throw new NullPointerException();
		schemaCheck(value.getType(), DEFAULT_VECTOR_TYPE);
	    values.add(value.getFloat());
		return this;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public FloatVector clone() {
		FloatVector newVector = new FloatVector();
		newVector.schema = schema;
		newVector.values = (List<Float>) ((ArrayList<Float>) values).clone();
		return newVector;
	}
	
	@Override
	public FloatVector fill(float value) {
		for(int i = 0; i < values.size(); i++) {
			values.set(i, value);
		}
		return this;
	}
	
	@Override
	public float getFloat(int index) {
		return values.get(index);
	}
	
	@Override
	public FloatValue getValue(int index) {
		return new FloatValue(values.get(index));
	}
	
	@Override
	public FloatVector setValue(int index, float value) {
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
	public Stream<FloatValue> stream() {
		return values.stream().map(FloatValue::new);
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
