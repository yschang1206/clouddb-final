package org.vanilladb.core.storage.log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.recovery.LogRecord;
import org.vanilladb.core.storage.tx.recovery.ReversibleIterator;

public class NVMLogMgr {
	private LogMgr logMgr = VanillaDb.logMgr();
	private Lock logMgrLock = new ReentrantLock();
	private Map<Long, List<LogRecord>> txLogListMap = 
			new ConcurrentHashMap<Long, List<LogRecord>>();
	
	/* non-volatile data structure */
	private NVMLogRingBuffer ringBuffer;
	private long globalLsn;
	
	final static String NV_DATA_STRUCTURE = "/home/yschang/buffer.bin";
	
	public NVMLogMgr() {
		File f = new File(NV_DATA_STRUCTURE);
		if (f.exists()) {
			try {
				DataInputStream dis = new DataInputStream(
						new FileInputStream(f));
				this.globalLsn = dis.readLong();
				int size = dis.readInt();
				int tailIdx = dis.readInt();
				long tailLsn = dis.readLong();
				int headIdx = dis.readInt();
				long headLsn = dis.readLong();
				dis.close();
				ringBuffer = new NVMLogRingBuffer(size, tailIdx, tailLsn, 
						headIdx, headLsn);
				ringBuffer.rebuild();
				System.out.println("Rebuild non-volatile data structure " +
				" globalLsn = " + globalLsn + " size = " + size + 
				" tailIdx = " + tailIdx + " tailLsn = " + tailLsn +
				" headIdx = " + headIdx + " headLsn = " + headLsn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			// XXX: initial head should be (-1, -1)
			this.globalLsn = 0;
			ringBuffer = new NVMLogRingBuffer(100000000, 0, 0, 0, 0);
		}
	}
	
	public void flush(LogSeqNum lsn) {
		ringBuffer.checkPersistence(lsn.val());
	}
	
	public LogSeqNum append(LogRecord rec) {
		/* add to per-tx list (volatile) */
		long txNum = rec.txNumber();
		List<LogRecord> list = txLogListMap.get(txNum);
		if (list == null) {
			list = new LinkedList<LogRecord>();
			txLogListMap.put(txNum, list);
		}
		list.add(rec);
		
		/* add to circular buffer (non-volatile) */
		logMgrLock.lock();
		long lsn = globalLsn++;
		logMgrLock.unlock();
		ringBuffer.insert(rec, lsn);
		return new LogSeqNum(lsn);
	}
	
	public void removeTxLogList(long txNum) {
		txLogListMap.remove(txNum);
	}
	
	public Iterator<LogRecord> getTxLogRecordIterator(long txNum) {
		List <LogRecord> list = txLogListMap.get(txNum);
		if (list == null)
			return null;
		return (new LinkedList<LogRecord>(list)).descendingIterator();
	}
	
	public ReversibleIterator<LogRecord> getLogRecordIterator() {
		return new NVMLogIterator(ringBuffer);
	}
 	
	public void persist() {
		DataOutputStream dos;
		try {
			dos = new DataOutputStream(new FileOutputStream(NV_DATA_STRUCTURE));
			dos.writeLong(globalLsn);
			dos.writeInt(ringBuffer.size());
			dos.writeInt(ringBuffer.tailIdx());
			dos.writeLong(ringBuffer.tailLsn());
			dos.writeInt(ringBuffer.headIdx());
			dos.writeLong(ringBuffer.headLsn());
			dos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ringBuffer.persist(logMgr);
	}
}
