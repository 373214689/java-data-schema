package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;


/**
 * Long Value
 * 
 * <li>2018/5/12 created by liuyang.</li>
 * @author liuyang
 * @version 1.0.0
 *
 */
public final class LongValue extends PrimitveValue {
	/** data type */
	private final static Type DEFAULT_VALUE_TYPE = Type.LONG;
	/** default value*/
	private final static long DEFALUT_VALUE = 0;
	
	long value;
	
    public LongValue(long value) {
    	this.value = value;
    }
    
    public LongValue() {
    	this(DEFALUT_VALUE);
    }
    
	@Override
	public final int compareTo(PrimitveValue other) {
		if (other == null) 
			throw new NullPointerException();
		if (!(other instanceof LongValue))
			throw new IllegalArgumentException("Illegal paramater: " + other + ", it is not " + getClass().getSimpleName());
		return Long.compare(value, other.getLong());
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
	public final long getLong() {
		return value;
    }
    
    @Override
    public final Long getValue() {
    	return value;
    }
    
    @Override
    public final void setValue(long value) {
    	this.value = value;
    }
    
    @Override
    public final int hashCode() {
    	return Long.hashCode(value);
    }
    
    @Override
    public final void modifyValue(Mode mode, long value) {
        switch (mode) {
        case INCREASE: this.value += value; break;
        case SUBTRACT: this.value -= value; break;
        case MULTIPLY: this.value *= value; break;
        case DIVISION: this.value /= value; break;
        case MODULO:   this.value %= value; break;
        case EXPONENT: this.value ^= value; break;
        case LOGARTHM: this.value = (long) (Math.log(this.value) / Math.log(value)); break;
		default:
			break;
        }
    }
    
    @Override
    public final int length() {
    	return 8;
    }
    
    @Override
    public String toString() {
      return String.valueOf(value);
    }
    
    @Override
    public final void updateValue(long newValue) {
    	value = newValue;
    }
    
    /*private byte[] getBytes() {
    	byte[] byteNum = new byte[8];  
        for (int i = 7; i >= 0; i--) {  
            byteNum[7 - i] = (byte) ((value >> i * 8) & 0xff);  
        }  
        return byteNum;
    }*/
    
    private final byte[] getBytes() {
    	return new byte[] { 
    			(byte) ((value >> 56) & 0xFF),  
    			(byte) ((value >> 48) & 0xFF),  
    			(byte) ((value >> 40) & 0xFF),  
    			(byte) ((value >> 32) & 0xFF),  
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
