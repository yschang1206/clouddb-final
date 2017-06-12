package org.vanilladb.core.storage.log;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.recovery.LogRecord;

public class NVMLogMgr {
	private LogMgr logMgr = VanillaDb.logMgr();
	private Lock logMgrLock = new ReentrantLock();
	private long globalLsn;
	
	public NVMLogMgr() {
		
	}
	
	public void flush(LogSeqNum lsn) {
		
	}
	
	public LogSeqNum append(LogRecord rec) {
		long lsn;
		
		logMgrLock.lock();
		lsn = globalLsn++;
		logMgrLock.unlock();
		return new LogSeqNum(lsn);
	}
	
	public void persist() {
		// TODO: serialize
	}
}
