namespace DbBCP
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Text;
	using System.Data.SqlClient;
	using System.Reflection;
	using System.Threading;
	using System.Windows;
	using System.Windows.Controls;
	using System.Windows.Data;
	using System.Windows.Documents;
	using System.Windows.Input;
	using System.Windows.Media;
	using System.Windows.Media.Imaging;
	using System.Windows.Navigation;
	using System.Windows.Shapes;
	using System.Diagnostics;
	using Common.Logging;


	public partial class MainWindow : Window
	{
		private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);


		public MainWindow()
		{
			InitializeComponent();
		}


		private void btnBulkCopy_Click(object sender, RoutedEventArgs e)
		{
			BcpWorker();
		}


		private void BcpWorker()
		{
			btnBulkCopy.IsEnabled = false;
			try {
				if (txtSourceServer.Text == "" || txtDestServer.Text == "" || txtSourceCatalog.Text == "" || txtDestCatalog.Text == "") {
					return;
				}


				Stopwatch sw = Stopwatch.StartNew();

				SqlConnectionStringBuilder cbSource = new SqlConnectionStringBuilder();
				cbSource.DataSource = txtSourceServer.Text;
				cbSource.InitialCatalog = txtSourceCatalog.Text;
				cbSource.IntegratedSecurity = (txtSourceUser.Text == "");
				if (!cbSource.IntegratedSecurity) {
					cbSource.UserID = txtSourceUser.Text;
					cbSource.Password = txtSourcePass.Text;
				}

				SqlConnectionStringBuilder cbDest = new SqlConnectionStringBuilder();
				cbDest.DataSource = txtDestServer.Text;
				cbDest.InitialCatalog = txtDestCatalog.Text;
				cbDest.IntegratedSecurity = (txtDestUser.Text == "");
				if (!cbDest.IntegratedSecurity) {
					cbDest.UserID = txtDestUser.Text;
					cbDest.Password = txtDestPass.Text;
				}

				SqlConnection connSource = new SqlConnection(cbSource.ConnectionString);
				connSource.Open();
				SqlConnection connDest = new SqlConnection(cbDest.ConnectionString);
				connDest.Open();


				IList<string> lstTables = new List<string>();
				SqlCommand cmdTables = new SqlCommand("SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS DbTable FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME", connSource);
				SqlDataReader rdr = cmdTables.ExecuteReader();
				while (rdr.Read()) {
					string tableName = rdr["DbTable"].ToString();
					lstTables.Add(tableName);
				}
				rdr.Close();


				IDictionary<String, Exception> dicFailedTables = new Dictionary<String, Exception>();
				foreach (string sTableName in lstTables) {
					//SqlTransaction trnDest = null;
					SqlBulkCopy sbc = null;
					try {
						SqlCommand cmdData = new SqlCommand("SELECT * FROM " + sTableName, connSource);
						rdr = cmdData.ExecuteReader();

						//TODO: fall back to DELETE FROM if TRUNCATE cannot be used, also tables need to be ordered by dependencies if there are FKs...
						//trnDest = connDest.BeginTransaction();
						try {
							//new SqlCommand("TRUNCATE TABLE " + sTableName, connDest, trnDest).ExecuteNonQuery();
							new SqlCommand("TRUNCATE TABLE " + sTableName, connDest).ExecuteNonQuery();
						} catch {
							//new SqlCommand("DELETE FROM " + sTableName, connDest, trnDest).ExecuteNonQuery();
							new SqlCommand("DELETE FROM " + sTableName, connDest).ExecuteNonQuery();
						}

						//sbc = new SqlBulkCopy(connDest, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, trnDest);
						//sbc = new SqlBulkCopy(connDest, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.UseInternalTransaction, null);
						sbc = new SqlBulkCopy(cbDest.ConnectionString, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.UseInternalTransaction);
						sbc.BulkCopyTimeout = 900;
						sbc.BatchSize = 10000;
						sbc.NotifyAfter = sbc.BatchSize;
						sbc.SqlRowsCopied += new SqlRowsCopiedEventHandler(sbc_SqlRowsCopied);
						sbc.DestinationTableName = sTableName;
						SqlBulkCopyColumnMappingCollection mapColumns = sbc.ColumnMappings;
						for (int i = 0; i < rdr.FieldCount; i++) {
							string sFieldName = rdr.GetName(i);
							mapColumns.Add(sFieldName, sFieldName);
						}
						sbc.WriteToServer(rdr);

						//trnDest.Commit();

					} catch (Exception ex) {
						dicFailedTables[sTableName] = ex;
						/*
						if (trnDest != null) {
							trnDest.Rollback();
						}
						*/

					} finally {
						if (sbc != null) {
							sbc.Close();
						}
						if (rdr != null) {
							rdr.Close();
						}
					}
				}

				connDest.Close();
				connSource.Close();

				sw.Stop();


				string msgCompleted = String.Format("Bulk copy operation completed in {0} ms", sw.Elapsed.TotalMilliseconds);
				log.Info(msgCompleted);

				if (dicFailedTables.Count == 0) {
					MessageBox.Show(msgCompleted);
				} else {
					string msgErrors = String.Format("The following tables failed to copy:");
					log.Error(msgErrors);
					foreach (KeyValuePair<string, Exception> kvp in dicFailedTables) {
						log.Error(String.Format("    [{0}] {1}", kvp.Key, kvp.Value.Message));
					}
					MessageBox.Show(String.Format("{0}\n\n{1} {2}", msgCompleted, msgErrors, String.Join(", ", dicFailedTables.Keys.ToArray())));
				}

			} catch (Exception ex) {
				log.Error(ex.Message, ex);

			} finally {
				btnBulkCopy.IsEnabled = true;
			}
		}


		private void sbc_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			log.Debug(String.Format("Copied up to {0} rows to {1}", e.RowsCopied, ((SqlBulkCopy)sender).DestinationTableName));
		}


	}

}
