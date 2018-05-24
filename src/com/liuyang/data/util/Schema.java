package com.liuyang.data.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Data Schema Class
 * @author liuyang
 *
 */
public final class Schema {
	
	/**
	 * Schema Type Defined
	 * <ul>
	 * Use one of follow
	 * <li> {@code BINARY} </li>
	 * <li> {@code BOOLEAN} </li>
	 * <li> {@code DOUBLE} </li>
	 * <li> {@code FLOAT} </li>
	 * <li> {@code INT} </li>
	 * <li> {@code LONG} </li>
	 * <li> {@code SHORT} </li>
	 * <li> {@code STRING} </li>
	 * <li> {@code STRUCT} </li>
	 * </ul>
	 * @version 1.0.0
	 * @author liuyang
	 *
	 */
	public static enum Type {
    	BYTE("byte", true),
    	BYTEARRAY("bytearray", true),
    	BINARY("binary", true),
    	BOOL("bool", true),
    	BOOLEAN("boolean", true),
    	CHAR("char", true),
    	DECIMAL("decimal", true),
    	DOUBLE("double", true),
    	FLOAT("float", true),
    	TINYINT("tinyint", true),
    	SMALLINT("smallint", true),
    	INT("int", true),
    	INTEGER("integer", true),
    	LIST("list", false),
    	BIGINT("bigint", true),
    	LONG("long", true),
    	MAP("map", false),
    	PRIMITVE("primitve", true),
    	STRUCT("struct", false),
    	/**短整数: short, smallint, 取值范围: -32768 - 32768*/
    	SHORT("short", true),
    	/**字符串: string, str*/
    	STRING("string", true),
    	UNION("union", false),
    	VARCHAR("varchar", true);
    	
    	private String name = null;
    	private boolean isPrimitive = false;
    	
    	private Type(String name, boolean isPrimitive) {
    		this.name = name;
    		this.isPrimitive = isPrimitive;
    	}

    	public String getName() {
    		return this.name;
    	}
    	
    	public boolean isPrimitive() {
    	      return isPrimitive;
    	}
    	
    	public String toString() {
    		return "{name: " + name + ", isPrimitive: " + isPrimitive + "}";
    	}
    	
    	/**
    	 * Try find the type of name, if not found, default use {@code Type.STRING} type.
    	 * @param name the type name, as {@code String}
    	 * @return the type {@link Schema.Type}
    	 */
    	public static Type find(String name) {
    		return Arrays.stream(values())
    				.filter(e -> e.name.equals(name))
    				.findFirst()
    				.orElse(STRING);
    	}
    }
	
	public static Schema create(String fieldName, String typeName) {
		return  new Schema(fieldName, Type.find(typeName));
	}
	
	public static Schema create(String fieldName, Type type) {
		return  new Schema(fieldName, type);
	}
	
	public static Schema createBoolean() {
        return new Schema(Type.BOOLEAN);
    }
    
    public static Schema createBoolean(String fieldName) {
        return new Schema(fieldName, Type.BOOLEAN);
    }
    
    public static Schema createByte() {
        return new Schema(Type.BYTE);
    }
    
    public static Schema createByte(String fieldName) {
        return new Schema(fieldName, Type.BYTE);
    }
    
    public static Schema createByteArray() {
        return new Schema(Type.BYTEARRAY);
    }
    
    public static Schema createByteArray(String fieldName) {
        return new Schema(fieldName, Type.BYTEARRAY);
    }
    
    public static Schema createChar() {
        return new Schema(Type.CHAR);
    }
    
    public static Schema createChar(String fieldName) {
        return new Schema(fieldName, Type.CHAR);
    }
    
    public static Schema createCharArray(int scale) {
        return new Schema(Type.CHAR);
    }
    
    public static Schema createDouble() {
        return new Schema(Type.DOUBLE);
    }
    
    public static Schema createDouble(String fieldName) {
        return new Schema(fieldName, Type.DOUBLE);
    }
    
    public static Schema createFloat(String fieldName) {
        return new Schema(fieldName, Type.FLOAT);
    }
    
    public static Schema createFloat() {
        return new Schema(Type.FLOAT);
    }
    
    public static Schema createInteger() {
        return new Schema(Type.INTEGER);
    }
    
    public static Schema createInteger(String fieldName) {
        return new Schema(fieldName, Type.INTEGER);
    }
    
    public static Schema createLong() {
        return new Schema(Type.LONG);
    }
    
    public static Schema createLong(String fieldName) {
        return new Schema(fieldName, Type.LONG);
    }
    
    public static Schema createShort() {
        return new Schema(Type.SHORT);
    }
    
    public static Schema createShort(String fieldName) {
        return new Schema(fieldName, Type.SHORT);
    }
    
    public static Schema createString() {
        return new Schema(Type.STRING);
    }
    
    public static Schema createString(String fieldName) {
        return new Schema(fieldName, Type.STRING);
    }
    
    public static Schema createStruct() {
        return new Schema(Type.STRUCT);
    }
    
    public static Schema createStruct(String fieldName) {
        return new Schema(fieldName, Type.STRUCT);
    }
    
	private Type type = null; 
	private String name = null;
	private String asName = null;
	private int    id = 0;
	private Schema parent = null;
	private List<String> fieldNames = null;
	private List<Schema> children = null;
	private int maxLength = 0;
	private int precision = 0;
	private int scale = 0;
    
    public Schema(String fieldName, Type fieldType, int precision, int scale) {
    	name = fieldName;
    	type = fieldType;
        if (type.isPrimitive) {
            children = null;
        } else {
            children = new ArrayList<>();
        }
        if (type == Type.STRUCT) {
            fieldNames = new ArrayList<>();
        } else {
            fieldNames = null;
        }
    }
    
	/**
	 * create a schema of column.
	 * @param fieldName the name of column.
	 * @param fieldType the type of column.
	 */
    public Schema(String fieldName, Type fieldType) {
    	name = fieldName;
    	type = fieldType;
        if (type.isPrimitive) {
            children = null;
        } else {
            children = new ArrayList<>();
        }
        if (type == Type.STRUCT) {
            fieldNames = new ArrayList<>();
        } else {
            fieldNames = null;
        }
    }
 
    public Schema(Type fieldType) {
    	this(null, fieldType);
    }
    
    @Override
    protected void finalize() {
    	type = null; 
    	name = null;
    	parent = null;
    	fieldNames = null;
    	children = null;
    	maxLength = 0;
    	precision = 0;
    	scale = 0;
    }
    
    /**
     * The alias name
     * @param asName
     * @return
     */
    public Schema as(String asName) {
    	this.asName = asName;
    	return this;
    }
    
    public Schema addField(Schema field) {
        if (type != Type.STRUCT) {
            throw new IllegalArgumentException("Can only add fields to struct type and not " + type);
        }
        children.add(field);
        fieldNames.add(field.name);
        field.id = children.size() - 1;
        field.parent = this;
        return this;
    }
    
    /**
     * 创建列缓冲区
     * @param maxSize 指定缓冲区长度
     * @return
     */
    /*@SuppressWarnings("rawtypes")
    public ColumnBuffer createColumn(int maxSize) {
    	switch(type) {
	    	case BYTE: {
	    		return new ByteColumn(maxSize, this);
		    }
	    	case BYTEARRAY: {
	    		return new ByteArrayColumn(maxSize, this);
		    }
	    	case CHAR: {
	    		return new CharColumn(maxSize, this);
		    }
	    	case DOUBLE: {
	    		return new DoubleColumn(maxSize, this);
		    }
	    	case INT: 
	    	case INTEGER: {
	    		return new IntegerColumn(maxSize, this);
		    }
	    	case LONG: {
	    		return new LongColumn(maxSize, this);
		    }
	    	case SHORT: {
	    		return new ShortColumn(maxSize, this);
		    }
	    	case STRING: {
	    		return new StringColumn(maxSize, this);
		    }
    	    case STRUCT: {
				ColumnBuffer[] fieldBuffer = new ColumnBuffer[children.size()];
    	    	for(int i=0; i < fieldBuffer.length; ++i) {
    	    		fieldBuffer[i] = children.get(i).createColumn(maxSize);
    	        }
    	        return new StructColumn(maxSize, this, fieldBuffer);
    	    }
    	    default:
    	        throw new IllegalArgumentException("Unknown type " + type);
    	}
    }*/
    
    /**
     * 创建列缓冲区
     * @return
     */
    /*@SuppressWarnings("rawtypes")
	public ColumnBuffer createColumn() {
    	return createColumn(ColumnBuffer.DEFAULT_SIZE);
    }*/
    
    
   /* public RowBuffer createRow() {
    	return null;
    }*/
    
    /**
     * Get the maximum length of the type. Only used for char and varchar types.
     * @return the maximum length of the string type
     */
    public int getMaxLength() {
        return maxLength;
    }

    /**
     * Get the precision of the decimal type.
     * @return the number of digits for the precision.
     */
    public int getPrecision() {
      return precision;
    }

    /**
     * Get the scale of the decimal type.
     * @return the number of digits for the scale.
     */
    public int getScale() {
      return scale;
    }
    
    public String getFieldName() {
    	return name;
    }
    
    public String getAsFieldName() {
    	return asName;
    }
    
    public int getFieldId() {
    	return id;
    }
    
    public Schema getParent() {
    	return parent;
    }
    
    public Type getType() {
    	return this.type;
    }
    
    public Type getType(int index) {
    	if (this.type == Type.STRUCT) return children.get(index).getType();
    	return this.type;
    }
    
    /**
     * For struct types, get the list of field names.
     * @return the list of field names.
     */
    public List<String> getFieldNames() {
        return Collections.unmodifiableList(fieldNames);
    }
    
    /**
     * Get the subtypes of this type.
     * @return the list of children types
     */
    public List<Schema> getChildren() {
    	return children == null ? null : Collections.unmodifiableList(children);
    }
    
    @Override
    public int hashCode() {
      long result = type.ordinal() * 4241 + maxLength + precision * 13 + scale;
      if (children != null) {
        for(Schema child: children) {
          result = result * 6959 + child.hashCode();
        }
      }
      return (int) result;
    }
    
    /**
     * Find the id of column with a field name. the struct column will be return the struct children id.
     * @param fieldName
     * @return If find a result, return the id of the result, otherwise return -1.
     */
    public int lookup(String fieldName) {
    	Schema target = find(fieldName);
    	return target == null ? -1 : target.getFieldId();
    }
    
    public Schema find(String fieldName) {
    	if (type == Type.STRUCT) {
    		return children.stream().filter(e -> e.name.equals(fieldName)).findFirst().orElse(null);
    	} else {
    		return fieldName.equals(name) ? this : null;
    	}
    }
    
    public Schema find(int fieldId) {
    	if (type == Type.STRUCT) {
    		return children.stream().filter(e -> fieldId == e.getFieldId()).findFirst().orElse(null);
    	} else {
    		return fieldId == id ? this : null;
    	}
    }
    
	public Schema select(String... fieldNames) {
		if (fieldNames == null) return this;
		if (fieldNames.length == 0) return this;
		if ("*".equals(fieldNames[0])) return this;
    	if (type == Type.STRUCT) {
    		Schema retval = createStruct(name);
    		for(String fieldName : fieldNames) {
    			Schema tmp = children.stream().filter(e -> e.name.equals(fieldName)).findFirst().orElse(null);
    			if (tmp != null) {
    				retval.addField(Schema.create(fieldName, tmp.type));
    			}
    		}
    		/*children.stream()
    		        .filter(e -> Arrays.stream(fieldNames).filter(f -> f.equals(e.name)).count() > 0)
    		        .forEach(e ->{
    		        	retval.addField(Schema.create(e.name, e.type));
    		        });*/
    		return retval;
    	} else {
    		return Arrays.stream(fieldNames).filter(f -> f.equals(name)).count() > 0 ? this : null;
    	}
    }
    
    /**
     * Set the name of this column.
     * @param newFieldName
     */
    public void setFieldName(String newFieldName) {
    	name = newFieldName;
    }
    
    public String toString() {
    	if (type == Type.STRUCT) {
    		return "struct " + name + " {" + 
    	            String.join(", ", children.stream().map(e -> e.toString()).toArray(s -> new String[s])) + "}";
    	} else {
    		return name + " : " + type.getName();
    	}
    }
    
}
