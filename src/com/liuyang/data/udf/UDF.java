package com.liuyang.data.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterator.OfInt;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

public class UDF {
	
	
	/*public static int rang(int a, int b, int step) {
		
	}*/
	/**
	 * Create an range from a to b.
	 * @param a
	 * @param b
	 * @return range from a to b
	 */
	public static IntStream range(int a, int b, int step, boolean parallel) {
		/*if (a < 0) 
			throw new IllegalArgumentException("the a can not be set 0.");
		if (b < 0) 
			throw new IllegalArgumentException("the b can not be set 0.");
		int min = Math.min(a, b);
		int max = Math.max(a, b);
		int size = max - min + 1;
		int[] arr = new int[size];
		for(int i = 0; i < size; i++) {
			arr[i] = min + i;
		}*/
		//IntRanger ranger = new IntRanger(a, b, 1);
		//Spliterators.
		return StreamSupport.intStream(
				 () -> new IntRanger(a, b, step)
				,Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED
				,parallel);
		/*return StreamSupport.intStream((OfInt) Spliterators.spliteratorUnknownSize(
				new IntRanger(a, b, 1), Spliterator.ORDERED | Spliterator.NONNULL), false);*/
		//return Arrays.stream(arr);
	}
	
	public static LongStream range(long a, long b) {
		if (a < 0) 
			throw new IllegalArgumentException("the a can not be set 0.");
		if (b < 0) 
			throw new IllegalArgumentException("the b can not be set 0.");
		long min = Math.min(a, b);
		long max = Math.max(a, b);
		int size = (int) (max - min + 1);
		long[] arr = new long[size];
		for(int i = 0; i < size; i++) {
			arr[i] = min + i;
		}
		//ArrayList a;
		return Arrays.stream(arr);
	}
	
	public static class IntRanger implements OfInt {
		int min;
		int max;
		int step;
		int cursor;
		
		public IntRanger(int a, int b, int step) {
			this.max = Math.max(a, b);
			this.min = Math.min(a, b);
			this.cursor = min;
			this.step = step;
		}
		
		@Override
		public long estimateSize() {
			return max - cursor;
		}

		@Override
		public int characteristics() {
			return Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED;
		}

		@Override
		public OfInt trySplit() {
			int remain = max - cursor, next = cursor + 10000;
			if (remain < 10000) return null;
			return new IntRanger(cursor = next, next, step);
		}

		@Override
		public boolean tryAdvance(IntConsumer action) {
			boolean hasNext = cursor < max;
			if (hasNext) action.accept(cursor++);
			return hasNext;
		}
		
		@Override
        public void forEachRemaining(Consumer<? super Integer> action) {
			do { } while(tryAdvance(action));
		}
		
	}
}
