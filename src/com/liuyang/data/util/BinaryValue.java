package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;

public final class BinaryValue extends PrimitveValue{
	/** data type */
	private final static Type DEFAULT_VALUE_TYPE = Type.BINARY;
	/** default value*/
	private final static byte[] DEFALUT_VALUE = new byte[] {};
	
    private byte[] value;
    
    public BinaryValue(byte[] value) {
    	this.value = value;
    }
    
    public BinaryValue() {
    	this(DEFALUT_VALUE);
    }
    
    @Override
    protected final void finalize() {
    	value = null;
    }
    
	@Override
	public final int compareTo(PrimitveValue other) {
		// byte数组不进行比较
		if (other == null) 
			throw new NullPointerException();
		if (!(other instanceof BinaryValue))
			throw new IllegalArgumentException("Illegal paramater: " + other + ", it is not " + getClass().getSimpleName());
		return 0;
	}
	
	@Override
	public final Type getType() {
		return DEFAULT_VALUE_TYPE;
	}
	
    @Override
    public final byte[] getBinary() {
    	return value;
    }
    
    @Override
    public byte[] getValue() {
    	return value;
    }
    
    @Override
    public final int hashCode() {
    	return value.hashCode();
    }
    
    @Override
    public final void setValue(byte[] value) {
    	this.value = value;
    }
    
    @Override
    public final String toString() {
    	return new String(value);
    }
    
    @Override
    public final void updateValue(byte[] newValue) {
    	value = newValue;
    }

    private final byte[] getBytes() {
    	return value;
    }
    
	@Override
	public final void writeValue(OutputStream o) throws IOException {
		o.write(getBytes());
	}
}
