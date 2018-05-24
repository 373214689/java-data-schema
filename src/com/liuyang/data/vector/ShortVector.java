package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.ShortValue;
import com.liuyang.data.util.Schema.Type;

public final class ShortVector extends PrimitveVector {
	/** vector type : FLOAT*/
	private final static Type DEFAULT_VECTOR_TYPE = Type.SHORT;
	
	private Schema schema;
	
	private List<ShortValue> values;

	public ShortVector(Schema schema, int initSize) {
		// 如果传入的Schema.type与Vector的type不匹配, 则抛出无效参数异常
		schemaCheck(schema.getType(), DEFAULT_VECTOR_TYPE, Type.SMALLINT);
		// 初始化
		this.schema = schema;
		this.values = new ArrayList<ShortValue>(initSize);
	}
	
	public ShortVector(Schema schema) {
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
    				String.format("Source schem type: %s can not match the ShortVector type %s"
    						,src
    						,String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
	@Override
	public final ShortVector append(short value) {
		values.add(new ShortValue(value));
		return this;
	}
	
	@Override
	public final ShortVector append(PrimitveValue value) {
		if (value == null) throw new NullPointerException();
		schemaCheck(value.getType(), DEFAULT_VECTOR_TYPE, Type.SMALLINT);
	    values.add((ShortValue) value);
		return this;
	}
	
	@Override
	public final ShortVector fill(short value) {
		for(int i = 0; i < values.size(); i++) {
			values.set(i, new ShortValue(value));
		}
		return this;
	}
	
	@Override
	public short getShort(int index) {
		return values.get(index).getShort();
	}
	
	@Override
	public ShortValue getValue(int index) {
		return values.get(index);
	}

	@Override
	public ShortVector setValue(int index, short value) {
		values.set(index, new ShortValue(value));
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
	public Stream<ShortValue> stream() {
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
