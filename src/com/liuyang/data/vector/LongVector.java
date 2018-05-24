package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
	
	private List<LongValue> values;

	public LongVector(Schema schema, int initSize) {
		// 如果传入的Schema.type与Vector的type不匹配, 则抛出无效参数异常
		schemaCheck(schema.getType(), DEFAULT_VECTOR_TYPE, Type.BIGINT);
		// 初始化
		this.schema = schema;
		this.values = new ArrayList<LongValue>(initSize);
	}
	
	public LongVector(Schema schema) {
		this(schema, 1);
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
		values.add(new LongValue(value));
		return this;
	}
	
	@Override
	public final LongVector append(PrimitveValue value) {
		if (value == null) throw new NullPointerException();
		schemaCheck(value.getType(), DEFAULT_VECTOR_TYPE, Type.BIGINT);
	    values.add((LongValue) value);
		return this;
	}
	
	
	@Override
	public final LongVector fill(long value) {
		for(int i = 0; i < values.size(); i++) {
			values.set(i, new LongValue(value));
		}
		return this;
	}
	
	@Override
	public final long getLong(int index) {
		return values.get(index).getLong();
	}
	
	@Override
	public final LongValue getValue(int index) {
		return values.get(index);
	}
	
	@Override
	public final LongVector setValue(int index, long value) {
		values.set(index, new LongValue(value));
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
