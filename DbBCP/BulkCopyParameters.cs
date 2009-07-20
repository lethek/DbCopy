using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace DbBCP
{

	public class BulkCopyParameters
	{
		public SqlConnectionStringBuilder Source = null;
		public SqlConnectionStringBuilder Destination = null;
		public readonly SortedList<string, long> Tables = new SortedList<string, long>();

		public BulkCopyParameters(SqlConnectionStringBuilder source, SqlConnectionStringBuilder destination, SortedList<string, long> tables)
		{
			Source = source;
			Destination = destination;
			Tables = tables;
		}
	}

}