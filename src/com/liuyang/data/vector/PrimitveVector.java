package com.liuyang.data.vector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.stream.Stream;

import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.Schema.Type;

/**
 * Primitve Vector Class
 * 
 * @author liuyang
 * @version 1.0.0
 *
 */
public abstract class PrimitveVector implements Comparable<PrimitveVector> {
	/** vector type */
	private final static Type DEFAULT_VECTOR_TYPE = Type.PRIMITVE;
	
	/**
	 * Append a boolean value to last of vector.
	 * @param value the value of {@link Boolean}
	 * @return get the pointer of this vector
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(boolean value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a byte[] value to last of vector.
	 * @param value the value of {@link byte[]}
	 * @return get the pointer of this vector
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(byte[] value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a double value to last of vector.
	 * @param value the value of {@link Double}
	 * @return get the pointer of this vector
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(double value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a float value to last of vector.
	 * @param value the value of {@link Float}
	 * @return get the pointer of this vector
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(float value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a int value to last of vector.
	 * @param value the value of {@link Integer}
	 * @return get the pointer of this vector
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(int value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a long value to last of vector.
	 * @param value the value of {@link Long}
	 * @return get the pointer of this vector
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(long value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a short value to last of vector.
	 * @param value the value of {@link Short}
	 * @return get the pointer of this vector
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(short value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a string value to last of vector.
	 * @param value the value of {@link String}
	 * @return get the pointer of this vector as {@link TextVector}
	 * 
     * @throws UnsupportedOperationException if the <tt>append</tt> operation
     *         is not supported by this vector
	 */
	public PrimitveVector append(String value) {
		throw new UnsupportedOperationException();
	}
	
	
	/**
	 * Append a primitve value to last of vector.
	 * @param value the value of {@link PrimitveValue}
	 * @return get the pointer of this vector
	 * 
     * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(PrimitveValue value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a vector value to last of vector.
	 * @param value the value of {@link PrimitveVector}
	 * @return get the pointer of this vector
	 * 
     * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(PrimitveVector value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Append a object value to vector.
	 * @param value the {@code Object} value
	 * @return get the pointer of this vector
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>append</tt> operation is not supported by this vector
	 */
	public PrimitveVector append(Object value) {
		if (value instanceof byte[]) {
			return append((byte[]) value);
		} else if (value instanceof Boolean) {
			return append((boolean) value);
		} else if (value instanceof Double) {
			return append((double) value);
		} else if (value instanceof Float) {
			return append((float) value);
		} else if (value instanceof Integer) {
			return append((int) value);
		} else if (value instanceof Long) {
			return append((long) value);
		} else if (value instanceof Short) {
			return append((short) value);
		} else if (value instanceof String) {
			return append((String) value);
		}
		throw new UnsupportedOperationException();
	}
	
    /**
     * Removes all of the elements from this vector (optional operation).
     * The vector will be empty after this method returns.
     *
     * @throws UnsupportedOperationException if the <tt>clear</tt> operation
     *         is not supported by this vector
     */
	public void clear() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Creates and returns a copy of this vector.
	 * 
	 * @return get the pointer of this vector
	 * 
     * @throws UnsupportedOperationException if the <tt>clone</tt> operation
     *         is not supported by this vector
	 */
	public PrimitveVector clone() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Compares the two specified {@code ColumnValue} values.
     *
     * @param   other   the value extends {@code PrimitveVector} to be compared.
     * @return  the value {@code 0} if this {@code PrimitveVector} is
     *          equal to the argument {@code PrimitveVector}; a value less than
     *          {@code 0} if this {@code PrimitveVector} is numerically less
     *          than the argument {@code PrimitveVector}; and a value greater than
     *           {@code 0} if this {@code PrimitveVector} is numerically
     *           greater than the argument {@code PrimitveVector} (signed
     *           comparison).
     * @since   1.2
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>compareTo</tt> operation is not supported by this vector
	 */
	public int compareTo(PrimitveVector other) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param value the value of {@link Boolean}
	 * @return get the pointer of this vector as {@link BooleanVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>fill</tt> operation is not supported by this vector
	 */
	public PrimitveVector fill(boolean value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param value the value of {@link byte[]}
	 * @return get the pointer of this vector as {@link BinaryVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>fill</tt> operation is not supported by this vector
	 */
	public PrimitveVector fill(byte[] value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param value the value of {@link Double}
	 * @return get the pointer of this vector as {@link DoubleVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>fill</tt> operation is not supported by this vector
	 */
	public PrimitveVector fill(double value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param value the value of {@link Float}
	 * @return get the pointer of this vector as {@link FloatVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>fill</tt> operation is not supported by this vector
	 */
	public PrimitveVector fill(float value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param value the value of {@link Integer}
	 * @return get the pointer of this vector as {@link IntegerVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>fill</tt> operation is not supported by this vector
	 */
	public PrimitveVector fill(int value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param value the value of {@link Long}
	 * @return get the pointer of this vector as {@link LongVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>fill</tt> operation is not supported by this vector
	 */
	public PrimitveVector fill(long value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param value the value of {@link Short}
	 * @return get the pointer of this vector as {@link ShortVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>fill</tt> operation is not supported by this vector
	 */
	public PrimitveVector fill(short value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param value the value of {@link String}
	 * @return get the pointer of this vector as {@link TextVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>fill</tt> operation is not supported by this vector
	 */
	public PrimitveVector fill(String value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Get the type of vector.
	 * @return result see {@link Schema.Type}
	 */
	public Type getType() {
		return DEFAULT_VECTOR_TYPE;
	}
	
	/**
	 * Get binary data.
	 * @return byte array data as {@code Binary}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getBinary</tt> operation is not supported by this vector
	 */
	public byte[] getBinary(int index) {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * Get string data.
	 * @return
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getString</tt> operation is not supported by this vector
	 */
	public String getString(int index) {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param index
	 * @return
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getInteger</tt> operation is not supported by this vector
	 */
	public int getInteger(int index) {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param index
	 * @return
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getLong</tt> operation is not supported by this vector
	 */
	public long getLong(int index) {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param index
	 * @return
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getBoolean/tt> operation is not supported by this vector
	 */
	public boolean getBoolean(int index) {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param index
	 * @return
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getFloat</tt> operation is not supported by this vector
	 */
	public float getFloat(int index) {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @param index
	 * @return
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getDouble</tt> operation is not supported by this vector
	 */
	public double getDouble(int index) {
	    throw new UnsupportedOperationException();
	}

	/**
	 * Get a short value from vector.
	 * @param index
	 * @return a short value
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getShort</tt> operation is not supported by this vector
	 */
	public short getShort(int index) {
	    throw new UnsupportedOperationException();
	}
	
	/**
	 * Get a integer value from vector.
	 * @param index
	 * @return a integer value
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>getValue</tt> operation is not supported by this vector
	 */
	public PrimitveValue getValue(int index) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * 
	 * @return
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>schema</tt> operation is not supported by this vector
	 */
	public Schema schema() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link Boolean}
	 * @return get the pointer of this vector as {@link BooleanValue}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, boolean value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link byte[]}
	 * @return get the pointer of this vector as {@link BinaryValue}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, byte[] value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link Double}
	 * @return get the pointer of this vector as {@link DoubleVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, double value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link Float}
	 * @return get the pointer of this vector as {@link FloatVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, float value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link Integer}
	 * @return get the pointer of this vector as {@link IntVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, int value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link Long}
	 * @return get the pointer of this vector as {@link LongVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, long value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link Object}
	 * @return get the pointer of this vector as {@link ObjectVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, Object value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link Short}
	 * @return get the pointer of this vector as {@link ShortVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, short value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Replaces the element at the specified position in this list with the specified element (optional operation).
	 * @param index index of the element to replace
	 * @param value element to be stored at the specified position, see {@link String}
	 * @return get the pointer of this vector as {@link TextVector}
	 * 
	 * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
	 */
	public PrimitveVector setValue(int index, String value) {
		throw new UnsupportedOperationException();
	}
	
	/**
     * Returns the number of elements in this vector.  If this vector
     * contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of elements in this vector
     * 
     * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
     */
	public int size() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Returns a sequential {@code Stream} with this vector as its source.
	 * 
     * @return a sequential {@code Stream} over the elements in this vector
     * 
     * @throws UnsupportedOperationException 
     *         if the <tt>setValue</tt> operation is not supported by this vector
     * @since 1.8
	 */
	public Stream<? extends PrimitveValue> stream() {
		throw new UnsupportedOperationException();
	}
	
	public PrimitveVector updateValue(int index, boolean value) {
		throw new UnsupportedOperationException();
	}
	
	public PrimitveVector updateValue(int index, byte[] value) {
		throw new UnsupportedOperationException();
	}
	
	public PrimitveVector updateValue(int index, double value) {
		throw new UnsupportedOperationException();
	}
	
	public PrimitveVector updateValue(int index, float value) {
		throw new UnsupportedOperationException();
	}
	
	public PrimitveVector updateValue(int index, int value) {
		throw new UnsupportedOperationException();
	}
	
	public PrimitveVector updateValue(int index, long value) {
		throw new UnsupportedOperationException();
	}
	
	public PrimitveVector updateValue(int index, short value) {
		throw new UnsupportedOperationException();
	}
	
	public PrimitveVector updateValue(int index, String value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Write this vector data to the specified output stream.
	 * @param o the specified {@code OutputStream}
	 * @throws IOException
	 */
	abstract public void writeValue(OutputStream o) throws IOException;
}
