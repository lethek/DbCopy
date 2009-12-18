﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using Common.Logging;

using DbCopy.Properties;

namespace DbCopy
{
	public partial class MainWindow : Window
	{
		private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
		private readonly BackgroundWorker worker = new BackgroundWorker();

		private bool delayedShutdown = false;
		private long currentTableRows = 0;


		public MainWindow()
		{
			InitializeComponent();

			//Load config
			txtSourceServer.Text = Settings.Default.SourceServer;
			txtSourceCatalog.Text = Settings.Default.SourceCatalog;
			txtSourceUser.Text = Settings.Default.SourceUsername;
			txtSourcePass.Text = Settings.Default.SourcePassword;
			txtDestServer.Text = Settings.Default.DestServer;
			txtDestCatalog.Text = Settings.Default.DestCatalog;
			txtDestUser.Text = Settings.Default.DestUsername;
			txtDestPass.Text = Settings.Default.DestPassword;

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
					SqlConnectionStringBuilder cbDest = new SqlConnectionStringBuilder {
						DataSource = txtDestServer.Text,
						InitialCatalog = txtDestCatalog.Text,
						IntegratedSecurity = (txtDestUser.Text == ""),
						UserID = txtDestUser.Text,
						Password = (txtDestUser.Text != "" ? txtDestPass.Text : ""),
						ConnectTimeout = 3
					};

					using (SqlConnection connDest = new SqlConnection(cbDest.ConnectionString))
					using (SqlCommand cmdDest = new SqlCommand(Query_SelectTableNames, connDest)) {
						connDest.Open();
						using (SqlDataReader reader = cmdDest.ExecuteReader()) {
							destTableNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
							while (reader.Read()) {
								destTableNames.Add(reader["TableNames"].ToString());
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
					Password = (txtSourceUser.Text != "" ? txtSourcePass.Text : "")
				};

				using (var connSource = new SqlConnection(cbSource.ConnectionString))
				using (var cmdSource = new SqlCommand(Query_SelectTableDetails, connSource)) {
					connSource.Open();
					using (SqlDataReader reader = cmdSource.ExecuteReader()) {
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

			} catch (Exception ex) {
				log.Error(ex.Message, ex);
				MessageBox.Show(ex.Message);
			}
		}


		private void btnBulkCopy_Click(object sender, RoutedEventArgs e)
		{
			if (txtSourceServer.Text == "" || txtDestServer.Text == "" || txtSourceCatalog.Text == "" || txtDestCatalog.Text == "") {
				return;
			}

			EnableForm(false);

			SqlConnectionStringBuilder cbSource = new SqlConnectionStringBuilder {
				DataSource = txtSourceServer.Text,
				InitialCatalog = txtSourceCatalog.Text,
				IntegratedSecurity = (txtSourceUser.Text == ""),
				UserID = txtSourceUser.Text,
				Password = (txtSourceUser.Text != "" ? txtSourcePass.Text : "")
			};

			SqlConnectionStringBuilder cbDest = new SqlConnectionStringBuilder {
				DataSource = txtDestServer.Text,
				InitialCatalog = txtDestCatalog.Text,
				IntegratedSecurity = (txtDestUser.Text == ""),
				UserID = txtDestUser.Text,
				Password = (txtDestUser.Text != "" ? txtDestPass.Text : "")
			};

			SortedList<string, long> tables = new SortedList<string, long>(lstTables.SelectedItems.Count);
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

			BulkCopyResult result = new BulkCopyResult();
			e.Result = result;

			BulkCopyParameters parameters = (BulkCopyParameters) e.Argument;

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

						reader = new SqlCommand(String.Format(Query_SelectAllInTable, sTableName), connSource) {CommandTimeout = 9000}.ExecuteReader();

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

						SqlBulkCopyColumnMappingCollection mapColumns = bulkCopy.ColumnMappings;
						for (int i = 0; i < reader.FieldCount; i++) {
							string sFieldName = reader.GetName(i);
							mapColumns.Add(sFieldName, sFieldName);
						}

						currentTableRows = parameters.Tables[sTableName];

						//Make sure the progress indicators are updated immediately, so the correct progress details are shown
						sbc_SqlRowsCopied(bulkCopy, new SqlRowsCopiedEventArgs(0));

						bulkCopy.WriteToServer(reader);

						transaction.Commit();

						log.Info(String.Format("Copied approximately {0} rows to {1}", parameters.Tables[sTableName], sTableName));

					} catch (Exception ex) {
						result.FailedTables[sTableName] = ex;
						if (transaction != null) {
							transaction.Rollback();
						}

					} finally {
						if (bulkCopy != null) {
							bulkCopy.Close();
						}
						if (reader != null) {
							reader.Close();
						}
					}

					if (worker.CancellationPending) {
						e.Cancel = true;
						return;
					}
				}
			} finally {
				if (connDest != null) {
					connDest.Close();
				}
				if (connSource != null) {
					connSource.Close();
				}
				sw.Stop();
				result.Elapsed = sw.Elapsed;
			}
		}


		private void BulkCopy_ProgressChanged(object sender, ProgressChangedEventArgs e)
		{
			barProgress.Value = (double)e.ProgressPercentage/1000;
			textProgress.Text = (string)e.UserState;
		}


		private void BulkCopy_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
		{
			EnableForm(true);
			btnCancel.Content = "_Cancel";

			if (e.Error != null) {
				log.Error(e.Error.Message, e.Error);
				MessageBox.Show(e.Error.Message);
			} else if (e.Cancelled) {
				log.Info("Bulk copy operation cancelled");
			} else {
				BulkCopyResult result = e.Result as BulkCopyResult;
				if (result == null) {
					return;
				}

				string msgCompleted = String.Format("Bulk copy operation completed in {0} ms", result.Elapsed.TotalMilliseconds);
				log.Info(msgCompleted);

				if (result.FailedTables.Count == 0) {
					MessageBox.Show(msgCompleted);
				} else {
					string msgErrors = String.Format("The following tables failed to copy:");
					log.Error(msgErrors);
					foreach (KeyValuePair<string, Exception> kvp in result.FailedTables) {
						log.Error(String.Format("    {0}: {1}", kvp.Key, kvp.Value.Message));
					}
					MessageBox.Show(String.Format("{0}\n\n{1} {2}", msgCompleted, msgErrors, String.Join(", ", result.FailedTables.Keys.ToArray())));
				}
			}

			barProgress.Value = barProgress.Minimum;
			textProgress.Text = "";

			if (delayedShutdown) {
				delayedShutdown = false;
				Close();
			}
		}


		private void sbc_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			log.Debug(String.Format("Copied up to {0} rows to {1}", e.RowsCopied, ((SqlBulkCopy) sender).DestinationTableName));

			double percentProgress = 100.0;
			if (currentTableRows > 0) {
				//Using Math.Min because its possible to calculate > 100% since neither row-counts can be guaranteed for accuracy
				percentProgress = Math.Min(((double)e.RowsCopied / currentTableRows * 100), 100.0);
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


		private void txtSource_TextChanged(object sender, TextChangedEventArgs e)
		{
			btnBulkCopy.IsEnabled = false;
			btnConnect.IsEnabled = CanConnect();
			lstTables.Items.Clear();
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
			Settings.Default.DestServer = txtDestServer.Text;
			Settings.Default.DestCatalog = txtDestCatalog.Text;
			Settings.Default.DestUsername = txtDestUser.Text;
			Settings.Default.DestPassword = txtDestPass.Text;
			Settings.Default.Save();

			if (worker.IsBusy && !worker.CancellationPending) {
				btnCancel.IsEnabled = false;
				btnCancel.Content = "Closing...";
				worker.CancelAsync();
				e.Cancel = true;
				delayedShutdown = true;
			}
		}


		private const string Query_SelectTableNames = @"
				select QUOTENAME(su.name) + '.' + QUOTENAME(so.name) as TableNames
				from sysobjects so
				inner join sysusers su on so.uid = su.uid
				where so.xtype = 'U'
				order by TableNames";

		private const string Query_SelectTableDetails = @"
				select QUOTENAME(su.name) + '.' + QUOTENAME(so.name) as TableNames, si.rows as Rows
				from sysobjects so
				inner join sysusers su on so.uid = su.uid
				inner join sysindexes si on si.id = so.id
				where si.indid < 2 and so.xtype = 'U'
				order by TableNames";

		private const string Query_SelectAllInTable = @"select * from {0}";

		private const string Query_DeleteAllInTable = @"delete from {0}";

		private const string Query_TruncateTable = @"truncate table {0}";
	}
}