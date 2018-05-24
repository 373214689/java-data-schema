package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;


public final class IntValue extends PrimitveValue {
	/** data type */
	private final static Type DEFAULT_VALUE_TYPE = Type.INTEGER;
	/** default value*/
	private final static int DEFALUT_VALUE = 0;
	
	int value;
	
    public IntValue(int value) {
    	this.value = value;
    }
    
    public IntValue() {
    	this(DEFALUT_VALUE);
    }
    
    
	@Override
	public final int compareTo(PrimitveValue other) {
		if (other == null) 
			throw new NullPointerException();
		if (!(other instanceof IntValue))
			throw new IllegalArgumentException("Illegal paramater: " + other + ", it is not " + getClass().getSimpleName());
		return Integer.compare(value, other.getInteger());
	}
	
	@Override
	public final boolean equals(Object anObject) {
		if (anObject == this) return true;
		if (anObject == null) return false;
		if (anObject instanceof IntValue) {
			return value == ((IntValue) anObject).value;
		} else if (anObject instanceof LongValue) {
			return value == ((LongValue) anObject).value;
		} else if (anObject instanceof ShortValue) {
			return value == ((ShortValue) anObject).value;
		} else if (anObject instanceof Integer) {
			return value == (Integer) anObject;
		} else if (anObject instanceof Long) {
			return value == (Long) anObject;
		} else if (anObject instanceof Short) {
			return value == (Short) anObject;
		}
		return false;
	}
	
	@Override
	public final Type getType() {
		return DEFAULT_VALUE_TYPE;
	}
	
    @Override
	public final int getInteger() {
		return value;
    }
    
    @Override
    public final Integer getValue() {
    	return value;
    }
    
    @Override
    public final int hashCode() {
    	return Integer.hashCode(value);
    }
    
    @Override
    public final void setValue(int value) {
    	this.value = value;
    }
    
    @Override
    public final String toString() {
      return String.valueOf(value);
    }
    
    @Override
    public final void updateValue(int newValue) {
    	value = newValue;
    }
    
    private final byte[] getBytes() {
    	return new byte[] {  
                (byte) ((value >> 24) & 0xFF),  
                (byte) ((value >> 16) & 0xFF),     
                (byte) ((value >> 8) & 0xFF),     
                (byte) (value & 0xFF)  
            };
    }

	@Override
	public final void writeValue(OutputStream o) throws IOException {
		o.write(getBytes());

	}
	


}
