using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;

using DbCopy.Properties;

using Serilog;


namespace DbCopy
{
	public partial class MainWindow : Window
	{

		public MainWindow()
		{
			InitializeComponent();

			//Load config
			txtSourceServer.Text = Settings.Default.SourceServer;
			txtSourceCatalog.Text = Settings.Default.SourceCatalog;
			txtSourceUser.Text = Settings.Default.SourceUsername;
			txtSourcePass.Text = Settings.Default.SourcePassword;
			chkSourceEncrypt.IsChecked = Settings.Default.SourceEncrypt;
			txtDestServer.Text = Settings.Default.DestServer;
			txtDestCatalog.Text = Settings.Default.DestCatalog;
			txtDestUser.Text = Settings.Default.DestUsername;
			txtDestPass.Text = Settings.Default.DestPassword;
			chkDestEncrypt.IsChecked = Settings.Default.DestEncrypt;

			txtCustomQuery.Text = Query_SelectAllInTable;

			worker.WorkerReportsProgress = true;
			worker.WorkerSupportsCancellation = true;
			worker.DoWork += BulkCopy_DoWork;
			worker.RunWorkerCompleted += BulkCopy_RunWorkerCompleted;
			worker.ProgressChanged += BulkCopy_ProgressChanged;
		}


		private void btnConnect_Click(object sender, RoutedEventArgs e)
		{
			if (!CanConnect()) {
				return;
			}

			HashSet<string> destTableNames = null;

			//Reset UI elements
			barProgress.Value = barProgress.Minimum;
			textProgress.Text = "";
			lstTables.Items.Clear();

			//TODO: move all this into a separate thread

			//Try connecting to the Destination database and retrieve its list of tables
			if (txtDestServer.Text.Length > 0 && txtDestCatalog.Text.Length > 0) {
				try {
					var cbDest = new SqlConnectionStringBuilder {
						DataSource = txtDestServer.Text.Trim(),
						InitialCatalog = txtDestCatalog.Text.Trim(),
						IntegratedSecurity = String.IsNullOrWhiteSpace(txtDestUser.Text),
						UserID = txtDestUser.Text.Trim(),
						Password = String.IsNullOrWhiteSpace(txtDestUser.Text) ? String.Empty : txtDestPass.Text.Trim(),
						Encrypt = chkDestEncrypt.IsChecked.HasValue && chkDestEncrypt.IsChecked.Value,
						ConnectTimeout = 3
					};

					using (var connDest = new SqlConnection(cbDest.ConnectionString)) {
						connDest.Open();
						var schema = connDest.GetSchema("Tables", new[] { connDest.Database });
						var tables = schema.AsEnumerable().Select(x => x.Field<string>("TABLE_SCHEMA") + "." + x.Field<string>("TABLE_NAME")).ToList();
						destTableNames = new HashSet<string>(tables, StringComparer.OrdinalIgnoreCase);
					}
				} catch (Exception) {
					//Ignore errors at this stage - indicate that destination table names are not available by setting the hashset to null
					destTableNames = null;
				}
			}


			//Read table names and schemas and approximate row-counts from the Source database and populate the listbox with them
			try {
				var cbSource = new SqlConnectionStringBuilder {
					DataSource = txtSourceServer.Text.Trim(),
					InitialCatalog = txtSourceCatalog.Text.Trim(),
					IntegratedSecurity = String.IsNullOrWhiteSpace(txtSourceUser.Text),
					UserID = txtSourceUser.Text.Trim(),
					Password = String.IsNullOrWhiteSpace(txtSourceUser.Text) ? String.Empty : txtSourcePass.Text.Trim(),
					Encrypt = chkSourceEncrypt.IsChecked.HasValue && chkSourceEncrypt.IsChecked.Value
				};

				using (var connSource = new SqlConnection(cbSource.ConnectionString)) {
					connSource.Open();
					string query = (GetEngineEdition(connSource) == SqlEngineEdition.Azure)
						? Query_SelectTableDetailsAzure
						: Query_SelectTableDetails;
					using (var cmdSource = new SqlCommand(query, connSource)) {
						using (var reader = cmdSource.ExecuteReader()) {
							while (reader.Read()) {
								var details = new TableDetails(reader["TableSchema"].ToString(), reader["TableName"].ToString(), reader["TableFullName"].ToString(), Convert.ToInt64(reader["RowCount"]));
								var item = new ListBoxItem {
									Content = details.FullName,
									Tag = details
								};

								//Colourize source table names depending on if they're found or not found in the destination db
								if (destTableNames != null) {
									if (!destTableNames.Contains($"{details.Schema}.{details.Name}")) {
										item.Foreground = Brushes.Red;
										item.FontWeight = FontWeights.Bold;
									} else {
										item.Foreground = Brushes.DarkBlue;
									}
								}

								lstTables.Items.Add(item);
							}
						}
					}
				}

			} catch (Exception ex) {
				Log.Error(ex, ex.Message);
				MessageBox.Show(ex.Message);
			}
		}


		private void btnBulkCopy_Click(object sender, RoutedEventArgs e)
		{
			if (String.IsNullOrWhiteSpace(txtSourceServer.Text) || String.IsNullOrWhiteSpace(txtDestServer.Text) ||
				String.IsNullOrWhiteSpace(txtSourceCatalog.Text) || String.IsNullOrWhiteSpace(txtDestCatalog.Text)) {
				return;
			}

			EnableForm(false);

			var cbSource = new SqlConnectionStringBuilder {
				DataSource = txtSourceServer.Text.Trim(),
				InitialCatalog = txtSourceCatalog.Text.Trim(),
				IntegratedSecurity = String.IsNullOrWhiteSpace(txtSourceUser.Text),
				UserID = txtSourceUser.Text.Trim(),
				Password = String.IsNullOrWhiteSpace(txtSourceUser.Text) ? String.Empty : txtSourcePass.Text.Trim(),
				Encrypt = chkSourceEncrypt.IsChecked.HasValue && chkSourceEncrypt.IsChecked.Value
			};

			var cbDest = new SqlConnectionStringBuilder {
				DataSource = txtDestServer.Text.Trim(),
				InitialCatalog = txtDestCatalog.Text.Trim(),
				IntegratedSecurity = String.IsNullOrWhiteSpace(txtDestUser.Text),
				UserID = txtDestUser.Text.Trim(),
				Password = String.IsNullOrWhiteSpace(txtDestUser.Text) ? String.Empty : txtDestPass.Text.Trim(),
				Encrypt = chkDestEncrypt.IsChecked.HasValue && chkDestEncrypt.IsChecked.Value
			};

			var tables = new SortedList<string, TableDetails>(lstTables.SelectedItems.Count);
			foreach (ListBoxItem listItem in lstTables.SelectedItems) {
				tables[listItem.Content.ToString()] = (TableDetails)listItem.Tag;
			}

			string query = expander.IsExpanded ? txtCustomQuery.Text.Trim() : Query_SelectAllInTable;

			worker.RunWorkerAsync(new BulkCopyParameters(cbSource, cbDest, tables, query));
		}


		private void btnCancel_Click(object sender, RoutedEventArgs e)
		{
			if (worker.IsBusy && !worker.CancellationPending) {
				worker.CancelAsync();
				btnCancel.IsEnabled = false;
				btnCancel.Content = "Cancelling...";
			}
		}


		private void BulkCopy_DoWork(object sender, DoWorkEventArgs e)
		{
			//TODO: use more usings!

			var sw = Stopwatch.StartNew();
			SqlConnection connSource = null;
			SqlConnection connDest = null;

			var result = new BulkCopyResult();
			e.Result = result;

			var parameters = (BulkCopyParameters)e.Argument;

			try {
				connSource = new SqlConnection(parameters.Source.ConnectionString);
				connDest = new SqlConnection(parameters.Destination.ConnectionString);

				connSource.Open();
				connDest.Open();

				SqlDataReader reader = null;
				foreach (var table in parameters.Tables.Values) {
					if (worker.CancellationPending) {
						e.Cancel = true;
						return;
					}

					Log.Information("Copying rows to {0}", "[" + connDest.DataSource + "].[" + connDest.Database + "]." + table.FullName);

					SqlTransaction transaction = null;
					SqlBulkCopy bulkCopy = null;
					try {
						//Use Dictionary<,> so that destination column names can be case-insensitively located in their proper case because SqlBulkCopy mappings are case-sensitive!
						var destSchema = connDest.GetSchema("Columns", new[] {connDest.Database, table.Schema, table.Name});
						var destColumnsMap = destSchema.AsEnumerable().Select(x => x.Field<string>("COLUMN_NAME")).ToDictionary(k => k, v => v, StringComparer.OrdinalIgnoreCase);

						transaction = connDest.BeginTransaction();

						string query = String.Format(parameters.Query, table.FullName);
						reader = new SqlCommand(query, connSource) { CommandTimeout = 9000 }.ExecuteReader();
						

						//TODO: any FKs should be dropped and then recreated after truncating
						try {
							new SqlCommand(String.Format(Query_TruncateTable, table.FullName), connDest, transaction) { CommandTimeout = 120 }.ExecuteNonQuery();
						} catch {
							new SqlCommand(String.Format(Query_DeleteAllInTable, table.FullName), connDest, transaction) { CommandTimeout = 120 }.ExecuteNonQuery();
						}

						bulkCopy = new SqlBulkCopy(connDest, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, transaction) {
							BulkCopyTimeout = 9000,
							BatchSize = 10000,
							NotifyAfter = 10000,
							DestinationTableName = table.FullName
						};
						bulkCopy.SqlRowsCopied += sbc_SqlRowsCopied;

						//Iterate over the Reader to get source column names because they may be defined by query results rather than a table-schema
						var sourceColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
						for (int i = 0; i < reader.FieldCount; i++) {
							sourceColumns.Add(reader.GetName(i));
						}

						var mapColumns = bulkCopy.ColumnMappings;
						foreach (var column in sourceColumns) {
							if (destColumnsMap.ContainsKey(column)) {
								mapColumns.Add(column, destColumnsMap[column]);
							}
						}

						rowsInCurrentTable = table.RowCount;

						//Make sure the progress indicators are updated immediately, so the correct progress details are shown
						sbc_SqlRowsCopied(bulkCopy, new SqlRowsCopiedEventArgs(0));

						bulkCopy.WriteToServer(reader);

						transaction.Commit();

						Log.Information("Copied approximately {0} rows to {1}", table.RowCount, "[" + connDest.DataSource + "].[" + connDest.Database + "]." + table.FullName);

					} catch (Exception ex) {
						result.FailedTables[table.FullName] = ex;
						transaction?.Rollback();
					} finally {
						bulkCopy?.Close();
						reader?.Close();
					}

					if (worker.CancellationPending) {
						e.Cancel = true;
						return;
					}
				}

			} finally {
				connDest?.Close();
				connSource?.Close();
				sw.Stop();
				result.Elapsed = sw.Elapsed;
			}
		}


		private void BulkCopy_ProgressChanged(object sender, ProgressChangedEventArgs e)
		{
			barProgress.Value = (double)e.ProgressPercentage / 1000;
			textProgress.Text = (string)e.UserState;
		}


		private void BulkCopy_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
		{
			EnableForm(true);
			btnCancel.Content = "_Cancel";

			if (e.Error != null) {
				Log.Error(e.Error, e.Error.Message);
				MessageBox.Show(e.Error.Message);

			} else if (e.Cancelled) {
				Log.Information("Bulk copy operation cancelled");

			} else {
				var result = e.Result as BulkCopyResult;
				if (result == null) {
					return;
				}

				string msgTemplate = "Bulk copy operation completed in {0} ms";
				Log.Information(msgTemplate, result.Elapsed.TotalMilliseconds);

				string msgCompleted = String.Format(msgTemplate, result.Elapsed.TotalMilliseconds);
				if (result.FailedTables.Count == 0) {
					MessageBox.Show(msgCompleted);
				} else {
					string msgErrors = "The following tables failed to copy:";
					Log.Error(msgErrors);
					foreach (var kvp in result.FailedTables) {
						Log.Error("    {0}: {1}", kvp.Key, kvp.Value);
					}
					MessageBox.Show($"{msgCompleted}\n\n{msgErrors} {String.Join(", ", result.FailedTables.Keys.ToArray())}");
				}
			}

			barProgress.Value = barProgress.Minimum;
			textProgress.Text = "";

			if (delayShutdown) {
				delayShutdown = false;
				Close();
			}
		}


		private void sbc_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			Log.Debug("Copied up to {0} rows to {1}", e.RowsCopied, ((SqlBulkCopy)sender).DestinationTableName);

			double percentProgress = 100.0;
			if (rowsInCurrentTable > 0) {
				//Using Math.Min because its possible to calculate > 100% since neither row-counts can be guaranteed for accuracy
				percentProgress = Math.Min(((double)e.RowsCopied / rowsInCurrentTable * 100), 100.0);
			}

			//Report progress to the UI; for increased granularity, multiply by 1000 and divide later since ReportProgress only supports integers
			worker.ReportProgress((int)(percentProgress * 1000), ((SqlBulkCopy)sender).DestinationTableName);

			if (worker.IsBusy && worker.CancellationPending) {
				e.Abort = true;
			}
		}


		/// <summary>
		/// Checks if a source has been specified (does not have to be a valid source).
		/// </summary>
		/// <returns>True if a source has been specified, False otherwise.</returns>
		private bool CanConnect()
		{
			return !String.IsNullOrWhiteSpace(txtSourceServer.Text) && !String.IsNullOrWhiteSpace(txtSourceCatalog.Text);
		}


		/// <summary>
		/// Checks if both source and destinations have been specified, that they are not identical and that 1 or more tables have been selected
		/// </summary>
		/// <returns>True if enough information has been provided to attempt to start a bulk-copy, False otherwise.</returns>
		private bool CanBulkCopy()
		{
			//TODO: testing whether source and destinations are identical by checking these string equalities is not sufficient, because a single server can be addressed in many different ways which would not be picked up here

			string srcServerText = txtSourceServer.Text.Trim();
			string dstServerText = txtDestServer.Text.Trim();
			string srcCatalogText = txtSourceCatalog.Text.Trim();
			string dstCatalogText = txtDestCatalog.Text.Trim();

			return CanConnect() &&
				lstTables.SelectedItems.Count > 0 &&
				!String.IsNullOrWhiteSpace(dstServerText) &&
				!String.IsNullOrWhiteSpace(dstCatalogText) &&
				!(
					String.Equals(srcServerText, dstServerText, StringComparison.InvariantCultureIgnoreCase) &&
					String.Equals(srcCatalogText, dstCatalogText, StringComparison.InvariantCultureIgnoreCase)
				);
			
		}


		private void lstTables_SelectionChanged(object sender, SelectionChangedEventArgs e)
		{
			btnBulkCopy.IsEnabled = CanBulkCopy();
		}


		private void txtDest_TextChanged(object sender, TextChangedEventArgs e)
		{
			btnBulkCopy.IsEnabled = CanBulkCopy();
		}


		private void chkDestEncrypt_OnChecked(object sender, RoutedEventArgs e)
		{
			btnBulkCopy.IsEnabled = CanBulkCopy();
		}


		private void txtSource_TextChanged(object sender, TextChangedEventArgs e)
		{
			btnBulkCopy.IsEnabled = false;
			btnConnect.IsEnabled = CanConnect();
			lstTables.Items.Clear();
		}


		private void chkSourceEncrypt_OnChecked(object sender, RoutedEventArgs e)
		{
			btnBulkCopy.IsEnabled = false;
			btnConnect.IsEnabled = CanConnect();
			lstTables.Items.Clear();
		}


		private void btnResetQuery_Click(object sender, RoutedEventArgs e)
		{
			txtCustomQuery.Text = Query_SelectAllInTable;
			txtCustomQuery.CaretIndex = txtCustomQuery.Text.Length;
			Keyboard.Focus(txtCustomQuery);
		}


		private void expander_Expanded(object sender, RoutedEventArgs e)
		{
			lstTables.SelectionMode = SelectionMode.Single;
		}


		private void expander_Collapsed(object sender, RoutedEventArgs e)
		{
			lstTables.SelectionMode = SelectionMode.Extended;
		}


		private void txtCustomQuery_OnIsVisibleChanged(object sender, DependencyPropertyChangedEventArgs e)
		{
			txtCustomQuery.CaretIndex = txtCustomQuery.Text.Length;
			Keyboard.Focus(txtCustomQuery);
		}


		private void Window_KeyDown(object sender, KeyEventArgs e)
		{
			if (e.Key == Key.Escape) {
				Close();
			}
		}


		private void EnableForm(bool enabled)
		{
			grpSource.IsEnabled = enabled;
			grpDest.IsEnabled = enabled;
			lstTables.IsEnabled = enabled;
			btnConnect.IsEnabled = enabled;
			btnBulkCopy.IsEnabled = enabled;
			btnCancel.IsEnabled = !enabled;
		}


		private void Window_Closing(object sender, CancelEventArgs e)
		{
			//Save config
			Settings.Default.SourceServer = txtSourceServer.Text.Trim();
			Settings.Default.SourceCatalog = txtSourceCatalog.Text.Trim();
			Settings.Default.SourceUsername = txtSourceUser.Text.Trim();
			Settings.Default.SourcePassword = txtSourcePass.Text.Trim();
			Settings.Default.SourceEncrypt = chkSourceEncrypt.IsChecked.HasValue && chkSourceEncrypt.IsChecked.Value;
			Settings.Default.DestServer = txtDestServer.Text.Trim();
			Settings.Default.DestCatalog = txtDestCatalog.Text.Trim();
			Settings.Default.DestUsername = txtDestUser.Text.Trim();
			Settings.Default.DestPassword = txtDestPass.Text.Trim();
			Settings.Default.DestEncrypt = chkDestEncrypt.IsChecked.HasValue && chkDestEncrypt.IsChecked.Value;
			Settings.Default.Save();

			if (worker.IsBusy && !worker.CancellationPending) {
				btnCancel.IsEnabled = false;
				btnCancel.Content = "Closing...";
				worker.CancelAsync();
				e.Cancel = true;
				delayShutdown = true;
			}
		}


		private SqlEngineEdition GetEngineEdition(SqlConnection conn)
		{
			using (var cmd = new SqlCommand(Query_SelectEngineEdition, conn)) {
				var edition = cmd.ExecuteScalar();
				if (edition != null) {
					return (SqlEngineEdition)edition;
				}
			}
			throw new Exception("Could not determine SQL Server edition");
		}


		private const string Query_SelectTableDetails = @"
				select
					su.name as TableSchema,
					so.name as TableName,
					QUOTENAME(su.name) + '.' + QUOTENAME(so.name) as TableFullName,
					si.rows as [RowCount]
				from sysobjects so
				inner join sysusers su on so.uid = su.uid
				inner join sysindexes si on si.id = so.id
				where si.indid < 2 and so.xtype = 'U'
				order by TableSchema, TableName";

		private const string Query_SelectTableDetailsAzure = @"
				select
					s.name as TableSchema,
					t.name as TableName,
					QUOTENAME(s.name) + '.' + QUOTENAME(t.name) as TableFullName,
					sum(ps.row_count) as [RowCount]
				from sys.tables t
				join sys.schemas s on s.schema_id = t.schema_id
				join sys.dm_db_partition_stats ps on ps.object_id = t.object_id
				where t.type = 'U'
				group by s.name, t.name
				order by TableSchema, TableName";

		private const string Query_SelectEngineEdition = @"select SERVERPROPERTY('EngineEdition')";

		private const string Query_SelectAllInTable = @"select * from {0}";

		private const string Query_DeleteAllInTable = @"delete from {0}";

		private const string Query_TruncateTable = @"truncate table {0}";


		private bool delayShutdown;

		private long rowsInCurrentTable;

		private readonly BackgroundWorker worker = new BackgroundWorker();

	}

}
