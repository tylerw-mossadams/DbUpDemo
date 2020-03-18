using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using NLog;
using System.Data.SqlClient;
using DbUp;
using DbUp.Helpers;
using Microsoft.SqlServer.Management.Smo;

namespace DbUpDemo
{
	public class DatabaseMigrator
	{
		private readonly IConfiguration _config;
		private static Logger _Logger = LogManager.GetCurrentClassLogger();
		private string _DatabaseName = "";
		private string _SqlLoginGroup = "";
		private string _ServerName = "";
		private string _ConnectionSting = "";

		public string BuildVersion { get; set; }

		public DatabaseMigrator(IConfiguration config)
		{
			_config = config;
			SetConnection(_config.GetConnectionString("DbUpDemo"));
			_SqlLoginGroup = _config.GetSection("AppSettings")["SqlLoginGroup"];
		}

		public void SetConnection(string connectionString)
		{
			_ConnectionSting = connectionString;
			var Conn = new SqlConnectionStringBuilder(connectionString);
			_ServerName = Conn.DataSource;
			_DatabaseName = Conn.InitialCatalog;
		}

		public int MigrateDatabase()
		{
			int ReturnValue = 0;

			_Logger.Trace("Begin database upgrade...");
			if (!DoesDatabaseExist())
			{
				_Logger.Trace("Creating new database.");
				CreateDatabase();
			}
			else
			{
				_Logger.Trace("Database already exists, backing up database.");
				BackupDatabase();
			}

			_Logger.Trace("Creating role");
			CreateRole();
			_Logger.Trace("Verifying logins.");
			EnsureLoginInApp();

			_Logger.Trace("Start schema changes.");
			var SchemaUpdater =
				DeployChanges.To
					.SqlDatabase(_ConnectionSting)
					.JournalToSqlTable("dbo", "_SchemaVersions")
					.WithScriptsEmbeddedInAssembly(typeof(Program).Assembly, s => IsSchemaScriptName(s))
					.LogToConsole()
					.Build();
			var SchemaUpdateResult = SchemaUpdater.PerformUpgrade();
			if (!SchemaUpdateResult.Successful)
			{
				Console.WriteLine(SchemaUpdateResult.Error);
				_Logger.Error(SchemaUpdateResult.Error.Message);
				Console.ForegroundColor = ConsoleColor.Red;
				Console.ResetColor();

				ReturnValue = -1;
			}

			var AlwaysSqlUpdater =
				DeployChanges.To
					.SqlDatabase(_ConnectionSting)
					.JournalTo(new NullJournal())
					.WithScriptsEmbeddedInAssembly(typeof(Program).Assembly, s => IsAlwaysSqlDataScriptName(s))
					.LogToConsole()
					.Build();
			var AlwaysSqlUpdaterResult = AlwaysSqlUpdater.PerformUpgrade();
			if (!AlwaysSqlUpdaterResult.Successful)
			{
				Console.WriteLine(AlwaysSqlUpdaterResult.Error);
				_Logger.Error(AlwaysSqlUpdaterResult.Error.Message);
				Console.ForegroundColor = ConsoleColor.Red;
				Console.ResetColor();

				ReturnValue = -1;
			}

			_Logger.Trace("Start data changes.");
			var SeedDataUpdater =
				DeployChanges.To
					.SqlDatabase(_ConnectionSting)
					.JournalTo(new NullJournal())
					.WithScriptsEmbeddedInAssembly(typeof(Program).Assembly, s => IsSeedDataScriptName(s))
					.LogToConsole()
					.Build();
			var SeedDataUpdateResult = SeedDataUpdater.PerformUpgrade();
			if (!SeedDataUpdateResult.Successful)
			{
				Console.WriteLine(SeedDataUpdateResult.Error);
				_Logger.Error(SeedDataUpdateResult.Error.Message);
				Console.ForegroundColor = ConsoleColor.Red;
				Console.ResetColor();
				ReturnValue = -1;
			}

			if (ReturnValue != 0)
			{
				_Logger.Trace("*** UPGRADE FAILED ***");
#if DEBUG
				Console.ReadLine();
#endif
			}
			else
			{
				SetDatabaseVersion();
				_Logger.Trace("Upgrade Successful");
			}
			return ReturnValue;
		}

		private void SetDatabaseVersion()
		{
			string VersionText = string.Format(@"
				IF EXISTS (SELECT 1 FROM fn_listextendedproperty(N'MA-LastUpdatedDate', NULL, NULL, NULL, NULL, NULL, NULL))
					EXEC sp_dropextendedproperty N'MA-LastUpdatedDate', NULL, NULL, NULL, NULL, NULL, NULL
				EXEC sp_addextendedproperty N'MA-LastUpdatedDate', '{0}', NULL, NULL, NULL, NULL, NULL, NULL
			", DateTime.Now.ToLocalTime().ToString("o"));

			if (!string.IsNullOrEmpty(BuildVersion))
			{
				VersionText += string.Format(@"
				IF EXISTS (SELECT 1 FROM fn_listextendedproperty(N'MA-Version', NULL, NULL, NULL, NULL, NULL, NULL))
					EXEC sp_dropextendedproperty N'MA-Version', NULL, NULL, NULL, NULL, NULL, NULL
				EXEC sp_addextendedproperty N'MA-Version', '{0}', NULL, NULL, NULL, NULL, NULL, NULL
			", BuildVersion);
			}
			using (SqlConnection sqlConn = new SqlConnection(_ConnectionSting))
			{
				sqlConn.Open();
				using (SqlCommand sqlComm = new SqlCommand(VersionText, sqlConn))
				{
					sqlComm.ExecuteNonQuery();
				}
			}
		}

		private void CreateDatabase()
		{
			string connString = string.Format("Server={0};Database=Master;Integrated Security=SSPI;", _ServerName);
			var str = string.Format(@"
				CREATE DATABASE {0};
				ALTER AUTHORIZATION ON DATABASE::{0} TO sa;
			", _DatabaseName);
			using (SqlConnection sqlConn = new SqlConnection(connString))
			{
				sqlConn.Open();
				using (SqlCommand sqlComm = new SqlCommand(str, sqlConn))
				{
					sqlComm.ExecuteNonQuery();
				}
			}
		}

		private void CreateRole()
		{
			var str = string.Format(@"
				IF DATABASE_PRINCIPAL_ID('MossAdamsAppAdmin') IS NULL
				BEGIN
					CREATE ROLE [MossAdamsAppAdmin];
				END;
			");
			using (SqlConnection sqlConn = new SqlConnection(_ConnectionSting))
			{
				sqlConn.Open();
				using (SqlCommand sqlComm = new SqlCommand(str, sqlConn))
				{
					sqlComm.ExecuteNonQuery();
				}
			}
		}

		private void EnsureLoginInApp()
		{
			if (_config.GetSection("AppSettings")["DeployPermsToAppDbFlag"] == "1")
			{
				EnsureLogin(_ServerName, _DatabaseName, _SqlLoginGroup, "MossAdamsAppAdmin");
			}
		}

		private void EnsureLogin(string serverName, string databaseName, string principalName, string roleName)
		{
			string connString = string.Format("Server={0};Database=Master;Integrated Security=SSPI;", serverName);
			var str = string.Format(@"
				IF NOT EXISTS 
					(SELECT name  
						FROM master.sys.server_principals
						WHERE name = '{0}')
				BEGIN
					CREATE LOGIN [{0}] FROM WINDOWS WITH DEFAULT_DATABASE=[{1}];
				END;
				
				USE {1};
				IF NOT EXISTS
					(SELECT name
						FROM sys.database_principals
						WHERE name = '{0}')
				BEGIN
					CREATE USER [{0}] FOR LOGIN [{0}];
				END;
				
				ALTER ROLE {2} ADD MEMBER [{0}];
			", principalName, databaseName, roleName);
			using (SqlConnection sqlConn = new SqlConnection(connString))
			{
				sqlConn.Open();
				using (SqlCommand sqlComm = new SqlCommand(str, sqlConn))
				{
					sqlComm.ExecuteNonQuery();
				}
			}
		}

		private void BackupDatabase()
		{
			string LastScriptName = "";

			try
			{
				using (SqlConnection Conn = new SqlConnection(_ConnectionSting))
				{
					Conn.Open();
					SqlCommand Cmd = Conn.CreateCommand();
					Cmd.CommandText = "SELECT TOP 1 ScriptName FROM _SchemaVersions ORDER BY Applied DESC";
					Cmd.CommandType = System.Data.CommandType.Text;
					using (SqlDataReader Rdr = Cmd.ExecuteReader())
					{
						if (Rdr.Read())
						{
							LastScriptName = Rdr.GetString(0).Split('.')[4];
						}
					}
				}
			}
			catch
			{
				LastScriptName = "MigratingFromV1";
			}

			string FilePath = Environment.CurrentDirectory + "\\" + LastScriptName + "_backup.bak";
			try
			{
				Server localServer = new Server();
				Backup backupMgr = new Backup();
				backupMgr.Devices.AddDevice(FilePath, DeviceType.File);
				backupMgr.CompressionOption = BackupCompressionOptions.On;
				backupMgr.Database = _DatabaseName;
				backupMgr.Action = BackupActionType.Database;
				backupMgr.FormatMedia = true;
				backupMgr.Initialize = true;
				backupMgr.SkipTapeHeader = true;
				backupMgr.SqlBackup(localServer);
			}
			catch (Exception ex)
			{
				_Logger.Error(ex, "Failed to backup the database.  ");
			}
		}

		private bool IsSeedDataScriptName(object s)
		{
			return s.ToString().Contains("DbUpDemo.Scripts.Seed.");
		}

		private bool IsAlwaysSqlDataScriptName(object s)
		{
			return s.ToString().Contains("DbUpDemo.Scripts.AlwaysSql.");
		}

		private bool IsSchemaScriptName(object s)
		{
			return s.ToString().StartsWith("DbUpDemo.Scripts.Schema.");
		}

		private bool DoesDatabaseExist()
		{
			string connString = string.Format("Server={0};Database=Master;Integrated Security=SSPI;", _ServerName);
			string commandText = string.Format("SELECT * from master.dbo.sysdatabases Where name=\'{0}\'", _DatabaseName);
			bool bRet = false;
			using (SqlConnection sqlConn = new SqlConnection(connString))
			{
				sqlConn.Open();
				using (SqlCommand sqlCmd = new SqlCommand(commandText, sqlConn))
				{
					using (SqlDataReader safeDr = sqlCmd.ExecuteReader())
					{
						if (safeDr.Read())
						{
							bRet = true;
						}
						else
						{
							bRet = false;
						}
					}
				}
			}
			return bRet;
		}
	}
}
