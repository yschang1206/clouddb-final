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
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.tx.recovery.LogRecord;
import org.vanilladb.core.storage.tx.recovery.ReversibleIterator;
import org.vanilladb.core.util.CoreProperties;

public class NVMLogMgr {
	private LogMgr logMgr = VanillaDb.logMgr();
	private Lock logMgrLock = new ReentrantLock();
	private Map<Long, List<LogRecord>> txLogListMap = 
			new ConcurrentHashMap<Long, List<LogRecord>>();
	
	/* non-volatile data structure */
	private NVMLogRingBuffer ringBuffer;
	private long globalLsn;
	
	private static final String NVM_DATA_STRUCTURE_FILE;
	private static final int NVM_RING_BUFFER_SIZE;
	static {
		NVM_DATA_STRUCTURE_FILE = CoreProperties.getLoader().getPropertyAsString(NVMLogMgr.class.getName() + ".NVM_DATA_STRUCTURE_FILE",
				"nvm.bin");
		NVM_RING_BUFFER_SIZE = CoreProperties.getLoader().getPropertyAsInteger(NVMLogMgr.class.getName() + ".NVM_RING_BUFFER_SIZE",
				10000000);
	}
	
	public NVMLogMgr() {
		File f = new File(FileMgr.getLogDirectoryPath(), NVM_DATA_STRUCTURE_FILE);
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
			this.globalLsn = 0;
			//ringBuffer = new NVMLogRingBuffer(4000000, 0, 0, 0, 0);
			ringBuffer = new NVMLogRingBuffer(NVM_RING_BUFFER_SIZE, 0, 0, 0, 0);
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
	
	public double utilization() {
		long headLsn = ringBuffer.headLsn();
		long tailLsn = ringBuffer.tailLsn();
		int size = ringBuffer.size();
		return (double)(tailLsn - headLsn) / (double)size;
	}

	public void checkpoint(List<Long> txNums) {
		ringBuffer.moveHeadForward(txNums);
	}
	
	public void persist() {
		try {
			DataOutputStream dos;
			File f = new File(FileMgr.getLogDirectoryPath(), NVM_DATA_STRUCTURE_FILE);
			dos = new DataOutputStream(new FileOutputStream(f));
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
