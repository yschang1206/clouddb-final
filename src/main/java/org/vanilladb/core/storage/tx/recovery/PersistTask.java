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

import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.util.CoreProperties;

/**
 * At first, we used this class to persist data structure in NVM periodically.
 * But since we've made this task a stored procedure, this class is now 
 * deprecated.
 */
public class PersistTask extends Task {
	private static Logger logger = Logger.getLogger(PersistTask.class
			.getName());
	private static final int NVM_PERSIST_PERIOD;
	static {
		NVM_PERSIST_PERIOD = CoreProperties.getLoader().getPropertyAsInteger(PersistTask.class.getName() + ".NVM_PERSIST_PERIOD",
				70000);
	}

	public PersistTask() {

	}

	@Override
	public void run() {
		try {
			Thread.sleep(NVM_PERSIST_PERIOD);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.info("Start persisting data structure in NVM");
		VanillaDb.nvmLogMgr().persist();
		logger.info("Data structure in NVM has been persisted");
	}
}
