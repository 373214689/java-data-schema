package com.liuyang.data.util;

import java.io.IOException;
import java.io.OutputStream;

import com.liuyang.data.util.Schema.Type;

/**
 * Primitve value
 * @author liuyang
 * @version 1.0.0
 *
 */
public abstract class PrimitveValue implements Comparable<PrimitveValue> {
	/** data type */
	private final static Type type = Type.PRIMITVE;
	
	public static enum Mode {
		APPEND,
		DELETE,
		INCREASE,
		SUBTRACT,
		MULTIPLY,
		DIVISION,
		MODULO,
		EXPONENT,
		LOGARTHM
	}
	
	public static PrimitveValue parse(Object o) {
		if (o instanceof byte[]) {
			return new BinaryValue((byte[]) o);
		} else if (o instanceof Integer) {
			return new IntValue((int) o);
		} else if (o instanceof Short) {
			return new ShortValue((short) o);
		} else if (o instanceof Long) {
			return new LongValue((long) o);
		} else if (o instanceof Float) {
			return new FloatValue((float) o);
		} else if (o instanceof Double) {
			return new DoubleValue((double) o);
		} else if (o instanceof String) {
			return new TextValue(o);
		} else {
			throw new IllegalArgumentException("Illegal paramater: " + o + ", can not parse it.");
		}
	}
	
	/**
	 * Compares the two specified {@code PrimitveValue} values.
	 * @param other the {@code PrimitveValue} to be compared.
	 * @return 
	 */
	public int compareTo(PrimitveValue other) {
		return 0;
	}
	
	/**
	 * Get the type of value.
	 * @return result see {@link Schema.Type}
	 */
	public Type getType() {
		return type;
	}
	
	/**
	 * Get binary data.
	 * @return byte array data as {@code Binary}
	 */
	public byte[] getBinary() {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * Get string data.
	 * @return
	 */
	public String getString() {
	    throw new UnsupportedOperationException();
	}
	
	public int getInteger() {
	    throw new UnsupportedOperationException();
	}
	
	public long getLong() {
	    throw new UnsupportedOperationException();
	}
	
	public boolean getBoolean() {
	    throw new UnsupportedOperationException();
	}
	
	public float getFloat() {
	    throw new UnsupportedOperationException();
	}
	
	public double getDouble() {
	    throw new UnsupportedOperationException();
	}

	public short getShort() {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * Get the length as this data used memery.
	 * the array and string data length will changed with content.
	 * <li>byte    : 1</li>
	 * <li>boolean : 1</li>
	 * <li>short   : 2</li>
	 * <li>int     : 4</li>
	 * <li>float   : 4</li>
	 * <li>long    : 8</li>
	 * <li>double  : 8</li>
	 * @return The length of data in memery.
	 */
	public int length() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Get the value as {@code Object} of Class (ex. String, Integer, Boolean ... and so on).
	 * @return the value
	 */
	public Object getValue() {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(int value) {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(long value) {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(double value) {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(float value) {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(boolean value) {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(byte[] value) {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(short value) {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(String value) {
		throw new UnsupportedOperationException();
	}
	
	public void setValue(Object value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param expression contains: >, <, ==, !=, >=, <=, in (...) not in (...)
	 * @param value
	 * @return
	 */
	public boolean filter(String expression, int value) {
		throw new UnsupportedOperationException();
	}
	
	public void updateValue(boolean newValue) {
		throw new UnsupportedOperationException();
	}
	
	public void updateValue(byte[] newValue) {
		throw new UnsupportedOperationException();
	}
	
	public void updateValue(double newValue) {
		throw new UnsupportedOperationException();
	}
	
	public void updateValue(float newValue) {
		throw new UnsupportedOperationException();
	}
	
	public void updateValue(int newValue) {
		throw new UnsupportedOperationException();
	}
	
	public void updateValue(long newValue) {
		throw new UnsupportedOperationException();
	}
	
	public void updateValue(short newValue) {
		throw new UnsupportedOperationException();
	}
	
	public void updateValue(String newValue) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Modify boolean data with the specified mode.
	 * @param mode 
	 * @param value
	 * @deprecated {@code modifyValue(Mode, boolean)} not implemented.
	 */
	@Deprecated
	public void modifyValue(Mode mode, boolean value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Modify byte array data with the specified mode.
	 * @param mode 
	 * @param value
	 * @deprecated {@code modifyValue(Mode, byte[])} not implemented.
	 */
	@Deprecated
	public void modifyValue(Mode mode, byte[] value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Modify double data with the specified mode.
	 * @param mode 
	 * @param value
	 */
	public void modifyValue(Mode mode, double value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Modify float data with the specified mode.
	 * @param mode 
	 * @param value
	 */
	public void modifyValue(Mode mode, float value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Modify int data with the specified mode.
	 * @param mode 
	 * @param value
	 */
	public void modifyValue(Mode mode, int value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Modify data with the specified mode.
	 * @param mode 
	 * @param value
	 */
	public void modifyValue(Mode mode, long value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Modify data with the specified mode.
	 * @param mode 
	 * @param value
	 */
	public void modifyValue(Mode mode, short value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Modify data with the specified mode.
	 * @param mode 
	 * @param value
	 * @deprecated {@code modifyValue(Mode, byte[])} not implemented.
	 */
	@Deprecated
	public void modifyValue(Mode mode, String value) {
		throw new UnsupportedOperationException();
	}
	
	
	/**
	 * Write data to the specified output stream.
	 * @param o the specified outputstream.
	 * @throws IOException
	 */
	abstract public void writeValue(OutputStream o) throws IOException;
	//abstract public void writeValue(RecordConsumer recordConsumer);
}
