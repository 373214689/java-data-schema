package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;

public final class TextValue extends PrimitveValue{
	/** data type */
	private final static Type DEFAULT_VALUE_TYPE = Type.STRING;
	/** default value*/
	private final static String DEFALUT_VALUE = "";
	
    String value;
    
    public TextValue(Object value) {
    	this.value = String.valueOf(value);
    }
    
    public TextValue(String value) {
    	this.value = value;
    }
    
    public TextValue() {
    	this(DEFALUT_VALUE);
    }
    
    @Override
    protected final void finalize() {
    	value = null;
    }
    
	@Override
	public final int compareTo(PrimitveValue other) {
		if (other == null) 
			throw new NullPointerException();
		if (!(other instanceof TextValue))
			throw new IllegalArgumentException("Illegal paramater: " + other + ", it is not " + getClass().getSimpleName());
		return value.compareTo(other.getString());
	}
	
	@Override
	public final boolean equals(Object anObject) {
		if (anObject == this) return true;
		if (anObject == null) return false;
		if (anObject instanceof TextValue) {
			TextValue other = (TextValue) anObject;
			if (value == other.value) return true;
			return value.equals(((TextValue) anObject).value);
		}
		return false;
	}
    
	@Override
	public final Type getType() {
		return DEFAULT_VALUE_TYPE;
	}
	
	@Override
	public final String getString() {
		return value;
	}
	
	@Override
	public String getValue() {
		return value;
	}
	
    @Override
    public final int hashCode() {
    	return (value == null) ? 0 : value.hashCode();
    }
    
    @Override
    public final int length() {
    	return value.getBytes().length;
    }

    @Override
    public void setValue(Object value) {
    	this.value = String.valueOf(value);
    }
    
    @Override
    public final void setValue(String value) {
    	this.value = String.valueOf(value);
    }
    
    @Override
    public final String toString() {
    	return value;
    }
    
    @Override
    public final void updateValue(String newValue) {
    	value = newValue;
    }
    
	@Override
	public final void writeValue(OutputStream o) throws IOException {
		o.write(value.getBytes());
	}
}
