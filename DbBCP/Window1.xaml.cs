using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using Common.Logging;

namespace DbBCP
{

	public partial class MainWindow : Window
	{
		private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);


		public MainWindow()
		{
			InitializeComponent();
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

			try {
				connSource.Open();

				SqlDataReader rdr = new SqlCommand(
					@"SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS DbTable FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME",
					connSource
				).ExecuteReader();

				while (rdr.Read()) {
					lstTables.Items.Add(new ListBoxItem {
						Content = rdr["DbTable"].ToString()
					});
				}
				rdr.Close();

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

			btnBulkCopy.IsEnabled = false;

			try {
				Stopwatch sw = Stopwatch.StartNew();

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

				SqlConnection connSource = new SqlConnection(cbSource.ConnectionString);
				connSource.Open();
				SqlConnection connDest = new SqlConnection(cbDest.ConnectionString);
				connDest.Open();

				SqlDataReader rdr = null;

				IDictionary<String, Exception> dicFailedTables = new Dictionary<String, Exception>();

				foreach (ListBoxItem itmTable in lstTables.SelectedItems) {
					string sTableName = itmTable.Content.ToString();

					//SqlTransaction trnDest = null;
					SqlBulkCopy sbc = null;
					try {
						rdr = new SqlCommand("SELECT * FROM " + sTableName, connSource).ExecuteReader();

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
						sbc = new SqlBulkCopy(connDest, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.UseInternalTransaction, null) {
							BulkCopyTimeout = 900,
							BatchSize = 10000,
							NotifyAfter = 10000,
							DestinationTableName = sTableName,
						};
						sbc.SqlRowsCopied += sbc_SqlRowsCopied;

						SqlBulkCopyColumnMappingCollection mapColumns = sbc.ColumnMappings;
						for (int i = 0; i < rdr.FieldCount; i++) {
							string sFieldName = rdr.GetName(i);
							mapColumns.Add(sFieldName, sFieldName);
						}

						sbc.WriteToServer(rdr);

						//trnDest.Commit();

					} catch (Exception ex) {
						dicFailedTables[sTableName] = ex;
						//if (trnDest != null) {
						//    trnDest.Rollback();
						//}

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


		private static void sbc_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			log.Debug(String.Format("Copied up to {0} rows to {1}", e.RowsCopied, ((SqlBulkCopy)sender).DestinationTableName));
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

	}

}
