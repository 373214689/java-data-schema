package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;


public final class BooleanValue extends PrimitveValue {
	/** data type */
	private final static Type DEFAULT_VALUE_TYPE = Type.BOOLEAN;
	/** default value*/
	private final static boolean DEFALUT_VALUE = false;
	
	boolean value;
	
    public BooleanValue(boolean value) {
    	this.value = value;
    }
    
    public BooleanValue() {
    	this(DEFALUT_VALUE);
    }
    
    @Override
    protected void finalize() {
    	value = false;
    }
    
	@Override
	public int compareTo(PrimitveValue other) {
		if (other == null) 
			throw new NullPointerException();
		if (!(other instanceof BooleanValue))
			throw new IllegalArgumentException("Illegal paramater: " + other + ", it is not " + getClass().getSimpleName());
		return Boolean.compare(value, other.getBoolean());
	}
	
	@Override
	public boolean equals(Object anObject) {
		if (anObject == this) return true;
		if (anObject == null) return false;
		if (anObject instanceof BooleanValue) {
			return value == ((BooleanValue) anObject).value;
		} else if (anObject instanceof Boolean) {
			return value == ((Boolean) anObject);
		}
		return false;
	}
	
	@Override
	public Type getType() {
		return DEFAULT_VALUE_TYPE;
	}
	
    @Override
	public boolean getBoolean() {
		return value;
    }
    
	@Override
	public Boolean getValue() {
		return value;
	}
    
    @Override
    public int hashCode() {
    	return Boolean.hashCode(value);
    }
    
    @Override
    public String toString() {
      return String.valueOf(value);
    }
    
    @Override
    public void updateValue(boolean newValue) {
    	value = newValue;
    }
    
    private byte[] getBytes() {
        return new byte[] { (byte) ((value ? 1 : 0) & 0xFF) };
    }

	@Override
	public void writeValue(OutputStream o) throws IOException {
		o.write(getBytes());

	}

}
