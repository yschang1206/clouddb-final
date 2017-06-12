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
package org.vanilladb.core.storage.log;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.Page;

public class LogSeqNum implements Comparable<LogSeqNum> {

	public static final int SIZE = Type.BIGINT.maxSize();
	
	public static final LogSeqNum DEFAULT_VALUE = new LogSeqNum(-1);

	private final long val;

	public static LogSeqNum readFromPage(Page page, int pos) {
		long val = (long) page.getVal(pos, Type.BIGINT).asJavaVal();

		return new LogSeqNum(val);
	}

	public LogSeqNum(long val) {
		this.val = val;
	}

	public long val() {
		return val;
	}

	public void writeToPage(Page page, int pos) {
		page.setVal(pos, new BigIntConstant(val));
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;

		if (!obj.getClass().equals(LogSeqNum.class))
			return false;

		LogSeqNum lsn = (LogSeqNum) obj;
		return lsn.val == this.val;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(val);
	}

	@Override
	public String toString() {
		return "[" + val + "]";
	}

	@Override
	public int compareTo(LogSeqNum lsn) {
		if (val < lsn.val)
			return -1;
		else if (val > lsn.val)
			return 1;
		return 0;
	}
}
