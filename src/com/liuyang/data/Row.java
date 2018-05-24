package com.liuyang.data;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import com.liuyang.data.udf.UDF;
import com.liuyang.data.util.BinaryValue;
import com.liuyang.data.util.BooleanValue;
import com.liuyang.data.util.DoubleValue;
import com.liuyang.data.util.FloatValue;
import com.liuyang.data.util.IntValue;
import com.liuyang.data.util.LongValue;
import com.liuyang.data.util.PrimitveValue;
import com.liuyang.data.util.Schema;
import com.liuyang.data.util.ShortValue;
import com.liuyang.data.util.TextValue;
import com.liuyang.data.util.Schema.Type;

public final class Row implements Iterable<PrimitveValue>, Comparable<Row> {
	
	public final static Row parseLine(Schema schema, String line, String delimiter) {
    	Row retval = new Row(schema);
    	int next = 0, pos = 0, size = retval.size;
    	String word;
    	for(int i = 0; i < size; i++) {
        	if ((pos = line.indexOf(delimiter, next)) != -1) {
        		word = line.substring(next, pos);
				next = pos + 1;
			} else {
				word = "";
			}
        	retval.parsePrimitveValue(i, word);
    	}
    	word = null;
    	return retval;
	}
	
    private int size = 0;
	
    private PrimitveValue[] values;
    
    private Schema schema;
    
    private Row() {
    	
    }
    
    public Row(Schema schema, boolean bInit) {
    	this.schema = schema;
    	this.size = schema.getType() == Schema.Type.STRUCT ? schema.getChildren().size() : 1;
    	values = new PrimitveValue[size];
    	if (bInit) {
    		UDF.range(0, size).forEach(i -> {
    			switch (schema.getType(i)) {
    			case BINARY:
    				values[i] = new BinaryValue() ; break;
    			case DOUBLE:
    				values[i] = new BooleanValue(); break;
    			case FLOAT:
    				values[i] = new FloatValue(); break;
    			case INT:
    			case INTEGER:
    				values[i] = new IntValue(); break;
    			case BIGINT:
    			case LONG:
    				values[i] = new LongValue(); break;
    			case SHORT:
    			case SMALLINT:
    				values[i] = new ShortValue(); break;
    			case STRING:
    			default:
    				values[i] = new TextValue(); break;
    			}
    		});
    	}
    }
    
    public Row(Schema schema) {
    	this(schema, false);
    }
    
    @Override
    protected final void finalize() {
    	size = 0;
    	values = null;
    	schema = null;
    }
    

	@Override
	public final int compareTo(Row other) {
		return 0;
	}
	
    /**
     * Returns <tt>true</tt> if this list contains the specified element.
     * More formally, returns <tt>true</tt> if and only if this list contains
     * at least one element <tt>e</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
     *
     * @param o element whose presence in this list is to be tested
     * @return <tt>true</tt> if this list contains the specified element
     */
    public final boolean contains(Object o) {
    	return indexOf(o) >= 0;
    }
    
    @Override
    public final Row clone() {
    	Row retval = new Row();
    	retval.schema = schema.select("*");
    	retval.values = pick("*");
    	return retval;
    }
    
    @Override
    public final boolean equals(Object anObject) {
		if (anObject == this) return true;
		if (anObject == null) return false;
		if (anObject instanceof Row) {
			Row other = (Row) anObject;
			if (!schema.equals(other.schema)) return false;
			return stream().filter(a -> other.contains(a)).count() == size;
		}
    	return false;
    }
    
    /**
     * Checks if the given index is in range.  If not, throws an appropriate
     * runtime exception.  This method does *not* check if the index is
     * negative: It is always used immediately prior to an array access,
     * which throws an ArrayIndexOutOfBoundsException if index is negative.
     * 
     * @param index
     */
    private final void rangeCheck(int index) {
        if (index >= size)
            throw new IndexOutOfBoundsException("index: " + index + ", Size: " + size);
    }
    
    /**
     * Check schema
     * @param index
     * @param types
     */
    private final void schemaCheck(int index, Type... types) {
    	Type type = schema.getType(index);
    	if (Arrays.stream(types).filter(e -> e.equals(type)).count() <= 0)
    		throw new IllegalArgumentException(
    				String.format("index: %d, type: %s can not match the %s", 
    						index, 
    						type,  
    						String.join(",", Arrays.stream(types).map(e -> e.getName()).toArray(n -> new String[n]))
    				)
    		);
    }
    
    /**
     * Get value like {@code PrimitveValue} with the specified index.
     * @param index
     * @return the result type is {@code PrimitveValue}
     */
    public final PrimitveValue get(int index) {
    	return values[index];
    }
    
    /**
     * Get value like {@code PrimitveValue} with the specified field name.
     * @param fieldName
     * @return the result type is {@code PrimitveValue}
     */
    public final PrimitveValue get(String fieldName) {
    	return values[schema.lookup(fieldName)];
    }
    
    public final byte[] getBinary(int index) {
    	rangeCheck(index);
    	schemaCheck(index, Type.BINARY);
    	return values[index].getBinary();
    }
    
    public final byte[] getBinary(String fieldName) {
    	return getBinary(schema.lookup(fieldName));
    }
    
    public final double getDouble(int index) {
    	rangeCheck(index);
    	schemaCheck(index, Type.DOUBLE);
    	return values[index].getDouble();
    }
    
    public final double getDouble(String fieldName) {
    	return getDouble(schema.lookup(fieldName));
    }
    
    public final float getFloat(int index) {
    	rangeCheck(index);
    	schemaCheck(index, Type.DOUBLE);
    	return values[index].getFloat();
    }
    
    public final float getFloat(String fieldName) {
    	return getFloat(schema.lookup(fieldName));
    }
    
    public final int getInteger(int index) {
    	rangeCheck(index);
    	schemaCheck(index, Type.INT, Type.INTEGER);
    	return values[index].getInteger();
    }
    
    public final int getInteger(String fieldName) {
    	return getInteger(schema.lookup(fieldName));
    }
    
    public final long getLong(int index) {
    	rangeCheck(index);
    	schemaCheck(index, Type.LONG, Type.BIGINT);
    	return values[index].getLong();
    }
    
    public final long getLong(String fieldName) {
    	return getLong(schema.lookup(fieldName));
    }
    
    public final short getShort(int index) {
    	rangeCheck(index);
    	schemaCheck(index, Type.SHORT, Type.SMALLINT);
    	return values[index].getShort();
    }
    
    public final short getShort(String fieldName) {
    	return getShort(schema.lookup(fieldName));
    }
    
    public final String getString(int index) {
    	rangeCheck(index);
    	schemaCheck(index, Type.SHORT, Type.SMALLINT);
    	return values[index].getString();
    }
    
    public final String getString(String fieldName) {
    	return getString(schema.lookup(fieldName));
    }
    
    public final PrimitveValue[] getValues() {
    	return values;
    }
    
    public final String getValueToString(int index) {
    	rangeCheck(index);
    	return String.valueOf(values[index]);
    }
    

    @Override
    public final int hashCode() {
    	if (schema.getType() == Type.STRUCT) {
    		int result = 0;
    		for(int i = 0; i < size; i++) {
    	          result = result * 31 + values[i].hashCode();
    	    }
    		return result;
    	} else {
    		return values[0].hashCode();
    	}
    	
    }
    
    /**
     * Returns the index of the first occurrence of the specified element
     * in this list, or -1 if this list does not contain the element.
     * More formally, returns the lowest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     */
    public final int indexOf(Object o) {
        if (o == null) {
            for (int i = 0; i < size; i++)
                if (values[i]==null)
                    return i;
        } else {
            for (int i = 0; i < size; i++)
                if (o.equals(values[i]))
                    return i;
        }
        return -1;
    }
    
    /**
     * Returns the index of the last occurrence of the specified element
     * in this list, or -1 if this list does not contain the element.
     * More formally, returns the highest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     */
    public final int lastIndexOf(Object o) {
        if (o == null) {
            for (int i = size-1; i >= 0; i--)
                if (values[i]==null)
                    return i;
        } else {
            for (int i = size-1; i >= 0; i--)
                if (o.equals(values[i]))
                    return i;
        }
        return -1;
    }
    
    public final Schema schema() {
    	return schema;
    }
    
    public final int size() {
    	return values.length;
    }
    
    public final Row select(Schema schema) {
    	if (schema == null) return this;
    	if (schema == this.schema) return this;
    	Row retval = new Row(schema);
    	List<String> fieldNames = schema.getFieldNames();
    	for (int i = 0; i < fieldNames.size(); i++) {
    		retval.set(i, get(fieldNames.get(i)));
    	}
    	return retval;
    }
    
    public final Row select(String... fieldNames) {
    	if (fieldNames == null) return this;
    	if (fieldNames.length == 0) return this;
    	if ("*".equals(fieldNames[0])) return this;
    	Row retval = new Row();
    	retval.schema = schema.select(fieldNames);
    	retval.values = pick(fieldNames);
    	return retval;
    }
    
    /*public Row pick(Schema schema) {
    	PrimitveValue[] retval = new PrimitveValue[fieldNames.length];
    	for(int i = 0; i < fieldNames.length; i++) {
    		retval[i] = get(fieldNames[i]);
    	}
    	return retval;
    }*/
    private final String nil(String value, String defaultValue) {
    	if (value == null) return defaultValue;
    	if (value.length() == 0) return defaultValue;
    	return value;
    }
    
    public final Row parseLine(String line, String delimiter) {
    	int next = 0, pos = 0;
    	String word;
    	for(int i = 0; i < size; i++) {
        	if ((pos = line.indexOf(delimiter, next)) != -1) {
        		word = line.substring(next, pos);
				next = pos + 1;
			} else {
				word = "";
			}
        	parsePrimitveValue(i, word);
    	}
    	//System.out.println(this);
    	word = null;
    	return this;
    }
    
    /**
     * Parse the value of primitve types.
     * @param index
     * @param value
     * @return
     */
    public final Row parsePrimitveValue(int index, String value) {
    	Type type = schema.getType(index);
    	switch(type) {
    	case BINARY: {
    		byte[] newValue = value.getBytes();
    		return updateValue(index, newValue);
    	}
    	case BOOLEAN: {
    		boolean newValue = Boolean.parseBoolean(nil(value, "false"));
    		return updateValue(index, newValue);
    	}
    	case DOUBLE: {
    		double newValue = Double.parseDouble(nil(value, "0.00"));
    		return updateValue(index, newValue);
    	}
    	case FLOAT: {
    		float newValue = Float.parseFloat(nil(value, "0.00"));
    		return updateValue(index, newValue);
    	}
    	case INT:
    	case INTEGER: {
    		int newValue = Integer.parseInt(nil(value, "0"));
    		return updateValue(index, newValue);
    	}
    	case BIGINT:
    	case LONG: {
    		long newValue = Long.parseLong(nil(value, "0"));
    		return  updateValue(index, newValue);
    	}
    	case SMALLINT:
    	case SHORT: {
    		short newValue = Short.parseShort(nil(value, "0"));
    		return  updateValue(index, newValue);
    	}
    	default:
    		return updateValue(index, value);
    	}
    }
    
    /**
     * Pick values with specfiy field names.
     * @param fieldNames
     * @return
     */
    public final PrimitveValue[] pick(String... fieldNames) {
    	if (fieldNames == null) return values.clone();
    	if (fieldNames.length == 0) return values.clone();
    	if ("*".equals(fieldNames[0])) return values.clone();
    	return Arrays.stream(fieldNames).map(this::get).toArray(n -> new PrimitveValue[n]);
    }
    
    public final Row set(int index, PrimitveValue value) {
    	values[index] = value;
    	return this;
    }
    
    public final Row set(String fieldName, PrimitveValue value) {
    	values[schema.lookup(fieldName)] = value;
    	return this;
    }
    
    public final Row setBinary(int index, byte[] value) {
    	return setValue(index, value);
    }
    
    
    public final Row setBinary(String fieldName, byte[] value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setDouble(int index, double value) {
    	return setValue(index, value);
    }
    
    public final Row setDouble(String fieldName, double value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setFloat(int index, float value) {
    	return setValue(index, value);
    }
    
    public final Row setFloat(String fieldName, float value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setInt(int index, int value) {
    	return setValue(index, value);
    }
    
    public final Row setInt(String fieldName, int value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setLong(int index, long value) {
    	return setValue(index, value);
    }
    
    public final Row setLong(String fieldName, long value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setShort(int index, short value) {
    	return setValue(index, value);
    }
    
    public final Row setShort(String fieldName, short value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setString(int index, Object value) {
    	return setValue(index, String.valueOf(value));
    }
    
    public final Row setString(String fieldName, Object value) {
    	return setValue(schema.lookup(fieldName), String.valueOf(value));
    }

    /**
     * Set binary value
     * @param index
     * @param value
     * @return
     */
    public final Row setValue(int index, byte[] value) {
    	rangeCheck(index);
    	schemaCheck(index, Type.BINARY);
    	return set(index, new BinaryValue(value));
    }
    
    public final Row setValue(String fieldName, byte[] value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setValue(int index, boolean value) {
    	rangeCheck(index);
    	schemaCheck(index, Type.BOOLEAN);
    	return set(index, new BooleanValue(value));
    }
    
    public final Row setValue(String fieldName, boolean value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setValue(int index, double value) {
    	rangeCheck(index);
    	schemaCheck(index, Type.DOUBLE);
    	return set(index, new DoubleValue(value));
    }
    
    public final Row setValue(String fieldName, double value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setValue(int index, float value) {
    	rangeCheck(index);
    	schemaCheck(index, Type.FLOAT);
    	return set(index, new FloatValue(value));
    }
    
    public final Row setValue(String fieldName, float value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Row setValue(int index, int value) {
    	rangeCheck(index);
    	schemaCheck(index, Type.INT, Type.INTEGER);
    	return set(index, new IntValue(value));
    }
    
    /**
     * Set integer value
     * @param fieldName
     * @param value
     * @return
     */
    public final Row setValue(String fieldName, int value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    /**
     * Set long value
     * @param index
     * @param value
     * @return
     */
    public final Row setValue(int index, long value) {
    	rangeCheck(index);
    	schemaCheck(index, Type.BIGINT, Type.LONG);
    	return set(index, new LongValue(value));
    }
    
    /**
     * Set long value
     * @param fieldName
     * @param value
     * @return
     */
    public final Row setValue(String fieldName, long value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    /**
     * Set short value
     * @param index
     * @param value the {@code short} type
     * @return
     */
    public final Row setValue(int index, short value) {
    	rangeCheck(index);
    	schemaCheck(index, Type.SHORT, Type.SMALLINT);
    	return set(index, new ShortValue(value));
    }
    
    /**
     * Set short value
     * @param fieldName
     * @param value
     * @return
     */
    public final Row setValue(String fieldName, short value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    /**
     * Set string value
     * @param index
     * @param value
     * @return
     */
    public final Row setValue(int index, String value) {
    	rangeCheck(index);
    	schemaCheck(index, Type.STRING);
    	return set(index, new TextValue(value));
    }
    
    /**
     * Set string value
     * @param fieldName
     * @param value
     * @return
     */
    public final Row setValue(String fieldName, String value) {
    	return setValue(schema.lookup(fieldName), value);
    }
    
    public final Stream<PrimitveValue> stream() {
    	return Arrays.stream(values);
    }
    
    @Override
    public final String toString() {
    	return "[" + String.join(",", stream().map(e -> e.toString()).toArray(n -> new String[n])) + "]";
    }

	@Override
	public final Iterator<PrimitveValue> iterator() {
		return Arrays.asList(values).iterator();
	}
	
	public final Row updateValue(int index, boolean newValue) {
		rangeCheck(index);
    	schemaCheck(index, Type.BOOLEAN);
    	if (values[index] == null) {
    		values[index] = new BooleanValue(newValue);
    	} else {
        	values[index].updateValue(newValue);
    	}
    	return this;
	}
	
	public final Row updateValue(int index, byte[] newValue) {
		rangeCheck(index);
    	schemaCheck(index, Type.BINARY);
    	if (values[index] == null) {
    		values[index] = new BinaryValue(newValue);
    	} else {
        	values[index].updateValue(newValue);
    	}
    	return this;
	}
	
	public final Row updateValue(int index, double newValue) {
		rangeCheck(index);
    	schemaCheck(index, Type.DOUBLE);
    	if (values[index] == null) {
    		values[index] = new DoubleValue(newValue);
    	} else {
        	values[index].updateValue(newValue);
    	}
    	return this;
	}
	
	public final Row updateValue(int index, float newValue) {
		rangeCheck(index);
    	schemaCheck(index, Type.FLOAT);
    	if (values[index] == null) {
    		values[index] = new FloatValue(newValue);
    	} else {
        	values[index].updateValue(newValue);
    	}
    	return this;
	}
	
	public final Row updateValue(int index, int newValue) {
		rangeCheck(index);
    	schemaCheck(index, Type.INT, Type.INTEGER);
    	if (values[index] == null) {
    		values[index] = new IntValue(newValue);
    	} else {
        	values[index].updateValue(newValue);
    	}
    	return this;
	}
	
	public final Row updateValue(int index, long newValue) {
		rangeCheck(index);
    	schemaCheck(index, Type.BIGINT, Type.LONG);
    	if (values[index] == null) {
    		values[index] = new LongValue(newValue);
    	} else {
        	values[index].updateValue(newValue);
    	}
    	return this;
	}
	
	public final Row updateValue(int index, short newValue) {
		rangeCheck(index);
    	schemaCheck(index, Type.SMALLINT, Type.SHORT);
    	if (values[index] == null) {
    		values[index] = new ShortValue(newValue);
    	} else {
        	values[index].updateValue(newValue);
    	}
    	return this;
	}
	
	public final Row updateValue(int index, String newValue) {
		rangeCheck(index);
    	schemaCheck(index, Type.STRING);
    	if (values[index] == null) {
    		values[index] = new TextValue(newValue);
    	} else {
        	values[index].updateValue(newValue);
    	}
    	return this;
	}

    
}
