package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.liuyang.data.util.BinaryValue;
import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.Schema.Type;

public final class BinaryVector extends PrimitveVector {
	/** vector type */
	private final static Type DEFAULT_VECTOR_TYPE = Type.BINARY;
	
	private Schema schema;
	
	private List<byte[]> values;

	public BinaryVector(Schema schema, int initSize) {
		// 如果传入的Schema.type与Vector的type不匹配, 则抛出无效参数异常
		schemaCheck(schema.getType(), DEFAULT_VECTOR_TYPE);
		// 初始化
		this.schema = schema;
		this.values = Collections.synchronizedList(new ArrayList<byte[]>(initSize));
	}
	
	public BinaryVector(Schema schema) {
		this(schema, 1);
	}
	
	private BinaryVector() {

	}

	@Override
	protected void finalize() {
		values.clear();
		schema = null;
		values = null;
	}
	
	@Override
	public BinaryVector append(byte[] value) {
		values.add(value);
		return this;
	}
	
    @Override
	public final BinaryVector append(Object value) {
		if (value instanceof byte[]) {
			return append((byte[]) value);
		}
		throw new UnsupportedOperationException();
	}
    
	/**
     * Check schema
     * @param index
     * @param types
     */
    private void schemaCheck(Type src, Type... types) {
    	if (Arrays.stream(types).filter(e -> e.equals(src)).count() <= 0)
    		throw new IllegalArgumentException(
    				String.format("Source schem type: %s can not match the BinaryVector type %s"
    						,src
    						,String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
	@Override
	public BinaryVector append(PrimitveValue value) {
		if (value == null) throw new NullPointerException();
		schemaCheck(value.getType(), DEFAULT_VECTOR_TYPE);
	    values.add(value.getBinary());
		return this;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public BinaryVector clone() {
		BinaryVector newVector = new BinaryVector();
		newVector.schema = schema;
		newVector.values = (List<byte[]>) ((ArrayList<byte[]>) values).clone();
		return newVector;
	}
	
	@Override
	public BinaryVector fill(byte[] value) {
		for(int i = 0; i < values.size(); i++) {
			values.set(i, value);
		}
		return this;
	}
	
	@Override
	public byte[] getBinary(int index) {
		return values.get(index);
	}
	
	@Override
	public Type getType() {
		return DEFAULT_VECTOR_TYPE;
	}
	
	@Override
	public BinaryValue getValue(int index) {
		return new BinaryValue(values.get(index));
	}
	
	@Override
	public BinaryVector setValue(int index, byte[] value) {
		values.set(index, value);
		return this;
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
	public Stream<BinaryValue> stream() {
		return values.stream().map(BinaryValue::new);
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
