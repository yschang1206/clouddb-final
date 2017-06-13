package org.vanilladb.core.storage.log;

import org.vanilladb.core.storage.tx.recovery.LogRecord;
import org.vanilladb.core.storage.tx.recovery.ReversibleIterator;

public class NVMLogIterator  implements ReversibleIterator<LogRecord> {
	private NVMLogRingBuffer ringBuffer;
	private int lower, upper;
	private int currentIdx;
	
	public NVMLogIterator(NVMLogRingBuffer ringBuffer) {
		this.ringBuffer = ringBuffer;
		this.lower = ringBuffer.headIdx();
		this.upper = ringBuffer.tailIdx();
		this.currentIdx = upper - 1;
	}
	
	@Override
	public boolean hasNext() {
		return (currentIdx > lower);
	}
	
	@Override
	public LogRecord next() {
		LogRecord rec = ringBuffer.get(currentIdx);
		currentIdx--;
		return rec;
	}
	
	@Override
	public boolean hasPrevious() {
		return (currentIdx < upper);
	}
	
	@Override
	public LogRecord previous() {
		LogRecord rec = ringBuffer.get(currentIdx);
		currentIdx++;
		return rec;
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
