/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.storage.tx.recovery;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

/**
 * The task performs non-quiescent checkpointing.
 */
public class CheckpointTask extends Task {
	private static Logger logger = Logger.getLogger(CheckpointTask.class
			.getName());

	private static final int TX_COUNT_TO_CHECKPOINT;
	private static final int METHOD_PERIODIC = 0, METHOD_MONITOR = 1, METHOD_NVM = 2;
	private static final int MY_METHOD;
	private static final long PERIOD;
	private long lastTxNum;
	private boolean first = true;

	static {
		TX_COUNT_TO_CHECKPOINT = CoreProperties.getLoader()
				.getPropertyAsInteger(CheckpointTask.class.getName()
						+ ".TX_COUNT_TO_CHECKPOINT", 1000);
		MY_METHOD = CoreProperties.getLoader().getPropertyAsInteger(
				CheckpointTask.class.getName() + ".MY_METHOD", METHOD_PERIODIC);
		PERIOD = CoreProperties.getLoader().getPropertyAsLong(
				CheckpointTask.class.getName() + ".PERIOD", 300000);
	}

	public CheckpointTask() {

	}

	/**
	 * Create a non-quiescent checkpoint.
	 */
	public void createCheckpoint() {
		boolean flag = false;
		if (MY_METHOD == METHOD_MONITOR) {
			if (VanillaDb.txMgr().getNextTxNum() - lastTxNum > TX_COUNT_TO_CHECKPOINT) {
				if (logger.isLoggable(Level.INFO))
					logger.info("Start creating checkpoint");
				flag = true;
				Transaction tx = VanillaDb.txMgr().newTransaction(
						Connection.TRANSACTION_SERIALIZABLE, false);
				VanillaDb.txMgr().createCheckpoint(tx);
				tx.commit();
				lastTxNum = VanillaDb.txMgr().getNextTxNum();
			}
		} else if (MY_METHOD == METHOD_NVM) {
			if (first || VanillaDb.nvmLogMgr().utilization() > 0.7) {
				if (logger.isLoggable(Level.INFO))
					logger.info("Start creating checkpoint");
				flag = true;
				first = false;
				Transaction tx = VanillaDb.txMgr().newTransaction(
						Connection.TRANSACTION_SERIALIZABLE, false);
				VanillaDb.txMgr().createCheckpoint(tx);
				tx.commit();
			}
			try {
				PrintWriter debug = new PrintWriter(new FileOutputStream("/home/yschang/debug.txt"), true);
				debug.println(VanillaDb.nvmLogMgr().utilization());
				debug.flush();
				debug.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else if (MY_METHOD == METHOD_PERIODIC) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Start creating checkpoint");
			flag = true;
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			VanillaDb.txMgr().createCheckpoint(tx);
			tx.commit();
		}
		if (flag && logger.isLoggable(Level.INFO))
			logger.info("A checkpoint created");
	}

	@Override
	public void run() {
		while (true) {
			createCheckpoint();
			try {
				Thread.sleep(PERIOD);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
