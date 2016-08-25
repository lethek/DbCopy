using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;

using DbCopy.Properties;

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
			//TODO: Shunt this off into a separate thread
			if (txtSourceServer.Text == "" || txtSourceCatalog.Text == "") {
				return;
			}

			HashSet<string> destTableNames = null;


			//Reset UI elements
			barProgress.Value = barProgress.Minimum;
			textProgress.Text = "";
			lstTables.Items.Clear();


			//Try connecting to the Destination database and retrieve its list of tables
			if (txtDestServer.Text.Length > 0 && txtDestCatalog.Text.Length > 0) {
				try {
					var cbDest = new SqlConnectionStringBuilder {
						DataSource = txtDestServer.Text,
						InitialCatalog = txtDestCatalog.Text,
						IntegratedSecurity = (txtDestUser.Text == ""),
						UserID = txtDestUser.Text,
						Password = (txtDestUser.Text != "" ? txtDestPass.Text : ""),
						Encrypt = chkDestEncrypt.IsChecked.HasValue && chkDestEncrypt.IsChecked.Value,
						ConnectTimeout = 3
					};

					using (var connDest = new SqlConnection(cbDest.ConnectionString)) {
						connDest.Open();
						string query = (GetEngineEdition(connDest) == SqlEngineEdition.Azure)
							? Query_SelectTableNamesAzure
							: Query_SelectTableNames;
						using (var cmdDest = new SqlCommand(query, connDest)) {
							using (var reader = cmdDest.ExecuteReader()) {
								destTableNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
								while (reader.Read()) {
									destTableNames.Add(reader["TableNames"].ToString());
								}
							}
						}
					}
				} catch (Exception) {
					//Ignore errors at this stage - indicate that destination table names are not available by setting the hashset to null
					destTableNames = null;
				}
			}


			//Read table names and schemas and approximate row-counts from the Source database and populate the listbox with them
			try {
				var cbSource = new SqlConnectionStringBuilder {
					DataSource = txtSourceServer.Text,
					InitialCatalog = txtSourceCatalog.Text,
					IntegratedSecurity = (txtSourceUser.Text == ""),
					UserID = txtSourceUser.Text,
					Password = (txtSourceUser.Text != "" ? txtSourcePass.Text : ""),
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
								string tableName = reader["TableNames"].ToString();
								var item = new ListBoxItem {
									Content = tableName,
									Tag = Convert.ToInt64(reader["Rows"])
								};

								//Colourize source table names depending on if they're found or not found in the destination db
								if (destTableNames != null) {
									if (!destTableNames.Contains(tableName)) {
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
			if (txtSourceServer.Text == "" || txtDestServer.Text == "" || txtSourceCatalog.Text == "" || txtDestCatalog.Text == "") {
				return;
			}

			EnableForm(false);

			var cbSource = new SqlConnectionStringBuilder {
				DataSource = txtSourceServer.Text,
				InitialCatalog = txtSourceCatalog.Text,
				IntegratedSecurity = (txtSourceUser.Text == ""),
				UserID = txtSourceUser.Text,
				Password = (txtSourceUser.Text != "" ? txtSourcePass.Text : ""),
				Encrypt = chkSourceEncrypt.IsChecked.HasValue && chkSourceEncrypt.IsChecked.Value
			};

			var cbDest = new SqlConnectionStringBuilder {
				DataSource = txtDestServer.Text,
				InitialCatalog = txtDestCatalog.Text,
				IntegratedSecurity = (txtDestUser.Text == ""),
				UserID = txtDestUser.Text,
				Password = (txtDestUser.Text != "" ? txtDestPass.Text : ""),
				Encrypt = chkDestEncrypt.IsChecked.HasValue && chkDestEncrypt.IsChecked.Value
			};

			var tables = new SortedList<string, long>(lstTables.SelectedItems.Count);
			foreach (ListBoxItem itmTable in lstTables.SelectedItems) {
				tables[itmTable.Content.ToString()] = Convert.ToInt64(itmTable.Tag);
			}

			worker.RunWorkerAsync(new BulkCopyParameters(cbSource, cbDest, tables));
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

			Stopwatch sw = Stopwatch.StartNew();
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
				foreach (string sTableName in parameters.Tables.Keys) {
					if (worker.CancellationPending) {
						e.Cancel = true;
						return;
					}

					SqlTransaction transaction = null;
					SqlBulkCopy bulkCopy = null;
					try {
						transaction = connDest.BeginTransaction();

						string query = String.Format(expander.IsExpanded ? txtCustomQuery.Text : Query_SelectAllInTable, sTableName);

						reader = new SqlCommand(query, connSource) { CommandTimeout = 9000 }.ExecuteReader();

						//TODO: any FKs should be dropped and then recreated after truncating
						try {
							new SqlCommand(String.Format(Query_TruncateTable, sTableName), connDest, transaction) { CommandTimeout = 120 }.ExecuteNonQuery();
						} catch {
							new SqlCommand(String.Format(Query_DeleteAllInTable, sTableName), connDest, transaction) { CommandTimeout = 120 }.ExecuteNonQuery();
						}

						bulkCopy = new SqlBulkCopy(connDest, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, transaction) {
							BulkCopyTimeout = 9000,
							BatchSize = 10000,
							NotifyAfter = 10000,
							DestinationTableName = sTableName
						};
						bulkCopy.SqlRowsCopied += sbc_SqlRowsCopied;

						var mapColumns = bulkCopy.ColumnMappings;
						for (int i = 0; i < reader.FieldCount; i++) {
							string sFieldName = reader.GetName(i);
							mapColumns.Add(sFieldName, sFieldName);
						}

						rowsInCurrentTable = parameters.Tables[sTableName];

						//Make sure the progress indicators are updated immediately, so the correct progress details are shown
						sbc_SqlRowsCopied(bulkCopy, new SqlRowsCopiedEventArgs(0));

						bulkCopy.WriteToServer(reader);

						transaction.Commit();

						Log.Info($"Copied approximately {parameters.Tables[sTableName]} rows to {sTableName}");

					} catch (Exception ex) {
						result.FailedTables[sTableName] = ex;
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
				Log.Info("Bulk copy operation cancelled");

			} else {
				var result = e.Result as BulkCopyResult;
				if (result == null) {
					return;
				}

				string msgCompleted = $"Bulk copy operation completed in {result.Elapsed.TotalMilliseconds} ms";
				Log.Info(msgCompleted);

				if (result.FailedTables.Count == 0) {
					MessageBox.Show(msgCompleted);
				} else {
					string msgErrors = "The following tables failed to copy:";
					Log.Error(msgErrors);
					foreach (KeyValuePair<string, Exception> kvp in result.FailedTables) {
						Log.Error($"    {kvp.Key}: {kvp.Value.Message}");
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
			Log.Debug($"Copied up to {e.RowsCopied} rows to {((SqlBulkCopy)sender).DestinationTableName}");

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
			return txtSourceServer.Text.Trim() != "" && txtSourceCatalog.Text.Trim() != "";
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

			//CanConnect check should be superfluous here due to the txtSource.TextChanged event, but it's added for clarity
			return CanConnect() &&
				lstTables.SelectedItems.Count > 0 &&
				dstServerText != "" &&
				dstCatalogText != "" &&
				!(srcServerText == dstServerText && srcCatalogText == dstCatalogText);
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
			Settings.Default.SourceServer = txtSourceServer.Text;
			Settings.Default.SourceCatalog = txtSourceCatalog.Text;
			Settings.Default.SourceUsername = txtSourceUser.Text;
			Settings.Default.SourcePassword = txtSourcePass.Text;
			Settings.Default.SourceEncrypt = chkSourceEncrypt.IsChecked.HasValue && chkSourceEncrypt.IsChecked.Value;
			Settings.Default.DestServer = txtDestServer.Text;
			Settings.Default.DestCatalog = txtDestCatalog.Text;
			Settings.Default.DestUsername = txtDestUser.Text;
			Settings.Default.DestPassword = txtDestPass.Text;
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


		private const string Query_SelectTableNames = @"
				select
					QUOTENAME(su.name) + '.' + QUOTENAME(so.name) as TableNames
				from sysobjects so
				inner join sysusers su on so.uid = su.uid
				where so.xtype = 'U'
				order by TableNames";

		private const string Query_SelectTableDetails = @"
				select
					QUOTENAME(su.name) + '.' + QUOTENAME(so.name) as TableNames,
					si.rows as Rows
				from sysobjects so
				inner join sysusers su on so.uid = su.uid
				inner join sysindexes si on si.id = so.id
				where si.indid < 2 and so.xtype = 'U'
				order by TableNames";

		private const string Query_SelectTableNamesAzure = @"
				select
					QUOTENAME(s.name) + '.' + QUOTENAME(t.name) as TableNames
				from sys.tables t
				join sys.schemas s on s.schema_id = t.schema_id
				where t.type = 'U'
				order by TableNames";

		private const string Query_SelectTableDetailsAzure = @"
				select
					QUOTENAME(s.name) + '.' + QUOTENAME(t.name) as TableNames,
					sum(ps.row_count) as Rows
				from sys.tables t
				join sys.schemas s on s.schema_id = t.schema_id
				join sys.dm_db_partition_stats ps on ps.object_id = t.object_id
				where t.type = 'U'
				group by s.name, t.name
				order by TableNames";

		private const string Query_SelectEngineEdition = @"select SERVERPROPERTY('EngineEdition')";

		private const string Query_SelectAllInTable = @"select * from {0}";

		private const string Query_DeleteAllInTable = @"delete from {0}";

		private const string Query_TruncateTable = @"truncate table {0}";


		private bool delayShutdown;

		private long rowsInCurrentTable;

		private readonly BackgroundWorker worker = new BackgroundWorker();

		private static readonly NLog.Logger Log = NLog.LogManager.GetCurrentClassLogger();

	}

}
