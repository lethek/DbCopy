namespace DbCopy
{

	public class TableDetails
	{
		public string Schema { get; private set; }
		public string Name { get; private set; }
		public string FullName { get; private set; }
		public long RowCount { get; private set; }


		public override string ToString() => FullName;


		public TableDetails(string schema, string name, string fullName, long rowCount)
		{
			Schema = schema;
			Name = name;
			FullName = fullName;
			RowCount = rowCount;
		}

	}

}