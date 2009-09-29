using System;
using System.Collections.Generic;

namespace DbCopy
{

	public class BulkCopyResult
	{
		public readonly IDictionary<String, Exception> FailedTables = new Dictionary<String, Exception>();
		public TimeSpan Elapsed { get; set; }
	}

}