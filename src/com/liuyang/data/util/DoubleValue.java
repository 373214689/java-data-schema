package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;


public final class DoubleValue extends PrimitveValue {
	/** data type */
	private final static Type DEFAULT_VALUE_TYPE = Type.DOUBLE;
	/** default value*/
	private final static double DEFALUT_VALUE = 0;
	
	double value;
	
    public DoubleValue(double value) {
    	this.value = value;
    }
    
    public DoubleValue() {
    	this(DEFALUT_VALUE);
    }
    
    @Override
    protected void finalize() {
    	value = 0.00;
    }
    
	@Override
	public final int compareTo(PrimitveValue other) {
		if (other == null) 
			throw new NullPointerException();
		if (!(other instanceof DoubleValue))
			throw new IllegalArgumentException("Illegal paramater: " + other + ", it is not " + getClass().getSimpleName());
		return Double.compare(value, other.getDouble());
	}
	
	@Override
	public final Type getType() {
		return DEFAULT_VALUE_TYPE;
	}
	
    @Override
	public final double getDouble() {
		return value;
    }
    
    @Override
    public final Double getValue() {
    	return value;
    }
    
    @Override
    public final int hashCode() {
    	return Double.hashCode(value);
    }
    
    @Override
    public final void modifyValue(Mode mode, double value) {
        switch (mode) {
        case INCREASE: this.value += value; break;
        case SUBTRACT: this.value -= value; break;
        case MULTIPLY: this.value *= value; break;
        case DIVISION: this.value /= value; break;
        case MODULO:   this.value %= value; break;
        case EXPONENT: this.value = Math.pow(this.value, value); break;
        case LOGARTHM: this.value = (Math.log(this.value) / Math.log(value)); break;
		default:
			break;
        }
    }
    
    @Override
    public final int length() {
    	return 8;
    }
    
    @Override
    public final void setValue(double value) {
    	this.value = value;
    }
    
    @Override
    public final String toString() {
      return String.valueOf(value);
    }
    
    @Override
    public final void updateValue(double newValue) {
    	value = newValue;
    }
    
    private final byte[] getBytes() {
    	long x = Double.doubleToLongBits(value);
    	return new byte[] { 
    			(byte) ((x >> 56) & 0xFF),  
    			(byte) ((x >> 48) & 0xFF),  
    			(byte) ((x >> 40) & 0xFF),  
    			(byte) ((x >> 32) & 0xFF),  
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
