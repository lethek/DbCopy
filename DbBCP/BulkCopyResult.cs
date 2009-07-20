using System;
using System.Collections.Generic;

namespace DbBCP
{

	public class BulkCopyResult
	{
		public readonly IDictionary<String, Exception> FailedTables = new Dictionary<String, Exception>();
		public TimeSpan Elapsed { get; set; }
	}

}