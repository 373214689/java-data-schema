package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;


public final class FloatValue extends PrimitveValue {
	/** data type */
	private final static Type DEFAULT_VALUE_TYPE = Type.FLOAT;
	/** default value*/
	private final static float DEFALUT_VALUE = 0;
	
	float value;
	
    public FloatValue(float value) {
    	this.value = value;
    }
    
    public FloatValue() {
    	this(DEFALUT_VALUE);
    }
    
	@Override
	public final int compareTo(PrimitveValue other) {
		if (other == null) 
			throw new NullPointerException();
		if (!(other instanceof FloatValue))
			throw new IllegalArgumentException("Illegal paramater: " + other + ", it is not " + getClass().getSimpleName());
		return Float.compare(value, other.getFloat());
	}
	
	@Override
	public final Type getType() {
		return DEFAULT_VALUE_TYPE;
	}
	
    @Override
	public final float getFloat() {
		return value;
    }
    
    @Override
    public final Float getValue() {
    	return value;
    }
    
    @Override
    public int hashCode() {
    	return Float.hashCode(value);
    }
    
    @Override
    public final void setValue(float value) {
    	this.value = value;
    }
    
    @Override
    public final String toString() {
      return String.valueOf(value);
    }
    
    @Override
    public final void updateValue(float newValue) {
    	value = newValue;
    }

    private final byte[] getBytes() {
    	int x = Float.floatToIntBits(value);
    	return new byte[] {  
                (byte) ((x >> 24) & 0xFF),  
                (byte) ((x >> 16) & 0xFF),     
                (byte) ((x >> 8) & 0xFF),     
                (byte) (x & 0xFF)  
            };
    }

	@Override
	public final void writeValue(OutputStream o) throws IOException {
		o.write(getBytes());

	}

}
