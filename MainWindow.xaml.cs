using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
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
			if (txtSourceServer.Text == "" || txtSourceCatalog.Text == "") {
				return;
			}

			SqlConnectionStringBuilder cbSource = new SqlConnectionStringBuilder {
				DataSource = txtSourceServer.Text,
				InitialCatalog = txtSourceCatalog.Text,
				IntegratedSecurity = (txtSourceUser.Text == ""),
				UserID = txtSourceUser.Text,
				Password = (txtSourceUser.Text != "" ? txtSourcePass.Text : "")
			};
			SqlConnection connSource = new SqlConnection(cbSource.ConnectionString);

			//TODO: connect to destination too and mark source tables red that are missing (don't show any error messages if this part fails) 

			try {
				connSource.Open();

				SqlDataReader reader = new SqlCommand(@"
					select QUOTENAME(su.name) + '.' + QUOTENAME(so.name) as TableNames, si.rows as Rows
					from sysobjects so
					inner join sysusers su on so.uid = su.uid
					inner join sysindexes si on si.id = so.id
					where si.indid < 2 and so.xtype = 'U'
					order by TableNames",
					connSource
					).ExecuteReader();

				while (reader.Read()) {
					lstTables.Items.Add(new ListBoxItem {
						Content = reader["TableNames"],
						Tag = Convert.ToInt64(reader["Rows"])
					});
				}
				reader.Close();
			} catch (Exception ex) {
				log.Error(ex.Message, ex);
				MessageBox.Show(ex.Message);
			} finally {
				connSource.Close();
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
				//TODO: tables need to be ordered by dependencies if there are FKs...
				foreach (string sTableName in parameters.Tables.Keys) {
					if (worker.CancellationPending) {
						e.Cancel = true;
						return;
					}

					SqlTransaction transaction = null;
					SqlBulkCopy bulkCopy = null;
					try {
						transaction = connDest.BeginTransaction();

						reader = new SqlCommand("SELECT * FROM " + sTableName, connSource) {CommandTimeout = 9000}.ExecuteReader();

						try {
							new SqlCommand("TRUNCATE TABLE " + sTableName, connDest, transaction) {CommandTimeout = 120}.ExecuteNonQuery();
						} catch {
							new SqlCommand("DELETE FROM " + sTableName, connDest, transaction) {CommandTimeout = 120}.ExecuteNonQuery();
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
			barProgress.Value = (double) e.ProgressPercentage/1000;
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
						log.Error(String.Format("    [{0}] {1}", kvp.Key, kvp.Value.Message));
					}
					MessageBox.Show(String.Format("{0}\n\n{1} {2}", msgCompleted, msgErrors, String.Join(", ", result.FailedTables.Keys.ToArray())));
				}
			}

			barProgress.Value = 0;

			if (delayedShutdown) {
				delayedShutdown = false;
				Close();
			}
		}


		private void sbc_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			log.Debug(String.Format("Copied up to {0} rows to {1}", e.RowsCopied, ((SqlBulkCopy) sender).DestinationTableName));

			double percentProgress = 100.0;
			if (e.RowsCopied > 0) {
				percentProgress = Math.Min(((double) e.RowsCopied/currentTableRows*100), 100.0); //Math.Min because its possible to get > 100% since neither figure can be guaranteed for accuracy
			}
			worker.ReportProgress((int) (percentProgress*1000)); //For increased granularity, multiply by 1000 and divide later since ReportProgress only supports integers

			if (worker.IsBusy && worker.CancellationPending) {
				e.Abort = true;
			}
		}


		private void lstTables_SelectionChanged(object sender, SelectionChangedEventArgs e)
		{
			btnBulkCopy.IsEnabled = (txtDestServer.Text != "" && txtDestCatalog.Text != "" && lstTables.SelectedItems.Count > 0);
		}


		private void txtDest_TextChanged(object sender, TextChangedEventArgs e)
		{
			btnBulkCopy.IsEnabled = (txtDestServer.Text != "" && txtDestCatalog.Text != "" && lstTables.SelectedItems.Count > 0);
		}


		private void txtSource_TextChanged(object sender, TextChangedEventArgs e)
		{
			btnBulkCopy.IsEnabled = false;
			btnConnect.IsEnabled = (txtSourceServer.Text != "" && txtSourceCatalog.Text != "");
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

	}
}