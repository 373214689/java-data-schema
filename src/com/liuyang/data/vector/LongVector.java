package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.liuyang.data.util.LongValue;
import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.Schema.Type;

public final class LongVector extends PrimitveVector {
	/** vector type : FLOAT*/
	private final static Type DEFAULT_VECTOR_TYPE = Type.LONG;
	
	private Schema schema;
	
	private List<Long> values;

	public LongVector(Schema schema, int initSize) {
		// 如果传入的Schema.type与Vector的type不匹配, 则抛出无效参数异常
		schemaCheck(schema.getType(), DEFAULT_VECTOR_TYPE, Type.BIGINT);
		// 初始化
		this.schema = schema;
		this.values = Collections.synchronizedList(new ArrayList<Long>(initSize));
	}
	
	public LongVector(Schema schema) {
		this(schema, 1);
	}
	
	private LongVector() {
		
	}

	@Override
	protected final void finalize() {
		values.clear();
		schema = null;
		values = null;
	}
	
    /**
     * Check schema
     * @param index
     * @param types
     */
    private final void schemaCheck(Type src, Type... types) {
    	if (Arrays.stream(types).filter(e -> e.equals(src)).count() <= 0)
    		throw new IllegalArgumentException(
    				String.format("Source schem type: %s can not match the LongVector type %s"
    						,src
    						,String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
	@Override
	public final LongVector append(long value) {
		values.add(value);
		return this;
	}
	
    @Override
	public final LongVector append(Object value) {
		if (value instanceof Long) {
			return append((long) value);
		}
		throw new UnsupportedOperationException();
	}
	
	@Override
	public final LongVector append(PrimitveValue value) {
		if (value == null) throw new NullPointerException();
		schemaCheck(value.getType(), DEFAULT_VECTOR_TYPE, Type.BIGINT);
	    values.add(value.getLong());
		return this;
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public LongVector clone() {
		LongVector newVector = new LongVector();
		newVector.schema = schema;
		newVector.values = (List<Long>) ((ArrayList<Long>) values).clone();
		return newVector;
	}
	
	@Override
	public final LongVector fill(long value) {
		for(int i = 0; i < values.size(); i++) {
			values.set(i, value);
		}
		return this;
	}
	
	@Override
	public final long getLong(int index) {
		return values.get(index);
	}
	
	@Override
	public final LongValue getValue(int index) {
		return new LongValue(values.get(index));
	}
	
	@Override
	public final LongVector setValue(int index, long value) {
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
	public Stream<LongValue> stream() {
		return values.stream().map(LongValue::new);
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
