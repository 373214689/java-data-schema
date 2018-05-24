package com.liuyang.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.liuyang.data.util.Schema;

/**
 * Data Set Class
 * 
 * @author liuyang
 * @version 1.0.0
 */
public class DataSet {
    private List<Row> rows;
    private Schema schema;
    
    private int limit = 20; // default limit number
    
    private DataSet() {
    	
    }
    
    /**
     * Create DataSet with the specified schema
     * Constructs an empty DataSet with the specified initial capacity.
     * <ul>Advice: it will be very fast if you give enough capacity. </ul>
     * @param schema specify the schema of dataset, see {@link Schema}
     * @param initialCapacity the initial capacity of the dataset
     */
    public DataSet(Schema schema, int initialCapacity) {
    	if (schema == null) 
    		throw new IllegalArgumentException("parameter schem can not be set null.");
    	this.schema = schema;
    	this.rows = new ArrayList<Row>(initialCapacity);
    }
    
    /**
     * Create DataSet with the specified schema
     * @param schema specify the schema of dataset, see {@link Schema}
     */
    public DataSet(Schema schema) {
    	this(schema, 0);
    }
    
    @Override
    protected final void finalize() {
    	rows.clear();
    	rows = null;
    	schema = null;
    }
    
    /**
     * Create one row with specified schema.
     * @param bAppend if true, initial default value for row element, otherwise set null for row element.
     * @return get a empty row, see {@link Row}
     */
    public final Row createRow() {
    	Row retval = new Row(schema, true);
    	rows.add(retval);
    	return retval;
    }
    
    public final void add(Row row) {
    	rows.add(row);
    }
    
    public final Row get(int index) {
    	return rows.get(index);
    }
    
    public final Row del(int index) {
    	return rows.remove(index);
    }
    
    public final boolean del(Row whichRow) {
    	return rows.remove(whichRow);
    }
    
    public final DataSet limit(int limit) {
    	this.limit = limit;
    	return this;
    }
    
    /**
     * Read text file
     * @param mapper use mapper convert one line as {@code String} to {@code Row}
     * @param distinct 
     * @param paths the file path
     * @return 
     */
    @SuppressWarnings("resource")
	public DataSet readTextFile(Function<? super String, ? extends Row> mapper, boolean bAppend, String... paths) {
    	if (paths == null) return this;
    	for(String path: paths) {
    		BufferedReader br;
    		try {
				br = new BufferedReader(new FileReader(path));
				br.lines().map(mapper).forEach(e -> {if (bAppend) add(e);});
				br.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				br = null;
			}
    	}
    	trimToSize();
    	return this;
    }
    
    /**
     * distinct data with specified field names.
     * use the hash set to distinct.
     * @param fieldNames can use {@code *} to match all field
     * @return
     */
    public DataSet distinct(String... fieldNames) {
    	DataSet tmp = select(fieldNames), retval = new DataSet(tmp.schema, tmp.size());
    	final HashSet<Row> sets = new HashSet<Row>();
    	tmp.stream().forEach(sets::add);
    	sets.forEach(retval::add);
    	retval.trimToSize();
    	tmp = null;
    	return retval;
    }
    
    
    public DataSet select(String... fieldNames) {
    	if (fieldNames == null) return this;
    	if (fieldNames.length == 0) return this;
    	if ("*".equals(fieldNames[0])) return this;
    	DataSet retval = new DataSet();
    	retval.schema = schema.select(fieldNames);
    	retval.rows = new ArrayList<Row>(size());
    	stream().map(e -> e.select(retval.schema)).forEach(retval::add);
    	return retval;
    }
    
    public int size() {
    	return rows.size();
    }
    
    public Schema schema() {
    	return schema;
    }
    
    public Stream<Row> stream() {
    	return rows.stream();
    }
    
    @Override
    public String toString() {
    	return "{" + String.join(",\r\n", stream().limit(limit).map(e -> e.toString()).toArray(n -> new String[n])) + "}";
    }
    
    /**
     * Trims the capacity of this <tt>DataSet</tt> instance to be the
     * list's current size.  An application can use this operation to minimize
     * the storage of an <tt>DataSet</tt> instance.
     */
    public void trimToSize() {
        ((ArrayList<Row>) rows).trimToSize(); 
    }

}
