package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;


public final class ShortValue extends PrimitveValue {
	/** data type */
	private final static Type DEFAULT_VALUE_TYPE = Type.SHORT;
	/** default value*/
	private final static short DEFALUT_VALUE = 0;
	
	short value;
	
    public ShortValue(short value) {
    	this.value = value;
    }
    
    public ShortValue() {
    	this(DEFALUT_VALUE);
    }
    
    @Override
    protected final void finalize() {
    	value = 0;
    }
    
	@Override
	public final int compareTo(PrimitveValue other) {
		if (other == null) 
			throw new NullPointerException();
		if (!(other instanceof ShortValue))
			throw new IllegalArgumentException("Illegal paramater: " + other + ", it is not " + getClass().getSimpleName());
		return Short.compare(value, other.getShort());
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
	public final short getShort() {
		return value;
    }
    
    @Override
    public final Short getValue() {
    	return value;
    }
    
    @Override
    public int hashCode() {
    	return Short.hashCode(value);
    }
    
    @Override
    public final void setValue(short value) {
    	this.value = value;
    }
    
    @Override
    public final String toString() {
      return String.valueOf(value);
    }
    
    @Override
    public final void updateValue(short newValue) {
    	value = newValue;
    }
    
    private byte[] getBytes() {
    	return new byte[] {  
                (byte) ((value >> 8) & 0xFF),     
                (byte) (value & 0xFF)  
            };
    }

	@Override
	public final void writeValue(OutputStream o) throws IOException {
		o.write(getBytes());

	}

}
