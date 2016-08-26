using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace DbCopy
{

	public class BulkCopyParameters
	{
		public SqlConnectionStringBuilder Source;
		public SqlConnectionStringBuilder Destination;
		public readonly SortedList<string, TableDetails> Tables;
		public string Query;

		public BulkCopyParameters(SqlConnectionStringBuilder source, SqlConnectionStringBuilder destination, SortedList<string, TableDetails> tables, string query)
		{
			Source = source;
			Destination = destination;
			Tables = tables;
			Query = query;
		}
	}

}
