package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.TextValue;
import com.liuyang.data.util.Schema.Type;

public final class TextVector extends PrimitveVector {
	/** vector type */
	private final static Type DEFAULT_VECTOR_TYPE = Type.STRING;
	
	private Schema schema;
	
	private List<String> values;

	
	private TextVector() {
		
	}
	
	public TextVector(Schema schema, int initSize) {
		// 如果传入的Schema.type与Vector的type不匹配, 则抛出无效参数异常
		schemaCheck(schema.getType(), DEFAULT_VECTOR_TYPE, Type.STRING);
		// 初始化
		this.schema = schema;
		this.values = Collections.synchronizedList(new ArrayList<String>(initSize));
	}
	
	public TextVector(Schema schema) {
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
    				String.format("Source schem type: %s can not match the TextVector type %s"
    						,src
    						,String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
    @Override
	public final TextVector append(Object value) {
		if (value instanceof String) {
			return append((String) value);
		}
		throw new UnsupportedOperationException();
	}
    
	/*@Override
	public final TextVector append(Object value) {
		values.add(String.valueOf(value));
		return this;
	}*/
    
	@Override
	public final TextVector append(String value) {
		values.add(value);
		return this;
	}
	
	@Override
	public final TextVector append(PrimitveValue value) {
		if (value == null) throw new NullPointerException();
		schemaCheck(value.getType(), DEFAULT_VECTOR_TYPE);
	    values.add(value.getString());
		return this;
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public TextVector clone() {
		TextVector newVector = new TextVector();
		newVector.schema = schema;
		newVector.values = (List<String>) ((ArrayList<String>) values).clone();
		return newVector;
	}
	
	@Override
	public TextVector fill(String value) {
		for(int i = 0; i < values.size(); i++) {
			values.set(i, value);
		}
		return this;
	}
	
	@Override
	public String getString(int index) {
		return values.get(index);
	}
	
	@Override
	public TextValue getValue(int index) {
		return new TextValue(values.get(index));
	}
	
	@Override
	public TextVector setValue(int index, String value) {
		values.set(index, value);
		return this;
	}
	
	@Override
	public TextVector setValue(int index, Object value) {
		values.set(index, String.valueOf(value));
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
	public Stream<TextValue> stream() {
		return values.stream().map(TextValue::new);
	}
	
	@Override
	public String toString() {
		if (values.size() > 10) {
			return "[" + String.join(", ", values.stream().limit(10).toArray(n -> new String[n])) + "...]";
		} else {
		    return "[" + String.join(", ", values.stream().toArray(n -> new String[n])) + "]";
		}
		
		
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
