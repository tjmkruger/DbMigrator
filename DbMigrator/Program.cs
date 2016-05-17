using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using CommandLine;
using CommandLine.Text;

namespace DbMigrator
{
    class Options
    {
        [Option('s', "server", Required = true, HelpText = "Server to connect to.")]
        public string Server { get; set; }

        [Option('d', "database", Required = true, HelpText = "Database to update.")]
        public string Database { get; set; }

        [Option('i', "integrated", DefaultValue = false, HelpText = "Use Integrated Security to connect")]
        public bool IntegratedSecurity { get; set; }

        [Option('u', "user", HelpText = "User to use to connect to the database.")]
        public string User { get; set; }

        [Option('p', "pass", HelpText = "Password to use for the user.")]
        public string Password { get; set; }

        [Option('m', "migrations", HelpText = "The folder to find the migrations files")]
        public string Migrations { get; set; }

        [Option('r', "routines", HelpText = "The folder to find the routine files")]
        public string Routines { get; set; }

        [Option('h', "history", DefaultValue = false, HelpText = "Show the migration history")]
        public bool ShowHistory { get; set; }

        [Option('l', "list", DefaultValue = false, HelpText = "List migrations that will run. Do not run the migraions now.")]
        public bool ListMigrations { get; set; }

        [Option('v', "verbose", DefaultValue = false, HelpText = "Show more information")]
        public bool Verbose { get; set; }

        [ParserState]
        public IParserState State { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            var help = new HelpText()
            {
                Heading = new HeadingInfo("Db Migrator", "1.0.0"),
                Copyright = new CopyrightInfo("RMS", 2015),
                AdditionalNewLineAfterOption = true,
                AddDashesToOption = true
            };
            help.AddPreOptionsLine("");
            help.AddPreOptionsLine("Usage: dbmigrator -p value");
            help.AddOptions(this);
            return help;
        }
    }

    class Program
    {
        static int Main(string[] args)
        {
            var options = new Options();

            if (!Parser.Default.ParseArguments(args, options))
            {
                Console.WriteLine("");
                Console.WriteLine("Errors: {0}", options.State.Errors.Count);
                
                foreach (var error in options.State.Errors.Where(error => error.ViolatesRequired))
                {
                    Console.WriteLine(error.BadOption.ShortName + " is required!");
                }

                Console.ReadLine();
                return -1;
            };

            if (options.ShowHistory)
                return PrintHistory(options);

            return RunMigration(options);
        }

        private static int RunMigration(Options options)
        {
            var runDate = DateTime.Now;
            Console.WriteLine("Migration started at: {0}", runDate);

            SqlConnection connection = null;
            SqlTransaction trans = null;
            var masterConnection = new SqlConnection(BuildMaserDbConnectionString(options));
            masterConnection.Open();

            try
            {
                EnsureDatabase(masterConnection, options.Database);
                
                connection = new SqlConnection(BuildConnectionString((options)));
                connection.Open();
                
                EnsureVersion(connection);
                trans = connection.BeginTransaction();

                MigrateTables(connection, trans, options, runDate);
                RemoveRoutines(connection, trans, runDate);
                CreateRoutines(connection, trans, options);
                trans.Commit();
                Console.WriteLine("Migration complete :)");
                return 0;
            }
            catch (Exception ex)
            {
                if (trans != null) trans.Rollback();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ResetColor();
                return -1;
            }
            finally
            {
                masterConnection.Close();
                if (connection != null) connection.Close();
            }
        }

        private static void EnsureVersion(SqlConnection connection)
        {
            const string ensureVersionStatement = @"
if not exists (select * from sys.objects where type = 'U' and name = '_Version')
begin
	create table _Version(
		Id int not null identity(1,1),
		Migration varchar(1000) not null,
		RunDate datetime not null
	)
end";

            var checkCmd = new SqlCommand(ensureVersionStatement, connection);
            checkCmd.ExecuteNonQuery();

            var history = GetHistory(connection, null).LastOrDefault();
            if (history != null)
                Console.WriteLine("Database is at version: {0} - {1}", history.Date, history.Name);
            else
                Console.WriteLine("Database is new.");
        }

        private static void EnsureDatabase(SqlConnection connection, string dbName)
        {
            var existsSatement = string.Format(@"Select database_id from sys.databases where name = '{0}'", dbName);
            var existsCmd = new SqlCommand(existsSatement, connection);
            var result = Convert.ToBoolean(existsCmd.ExecuteScalar());
            if (result)
            {
                Console.WriteLine("Found database: {0}", dbName);
                return;
            }

            Console.WriteLine("Creating dabase: {0}", dbName);
            var createStatement = string.Format(@"Create Database {0}", dbName);
            var cmd = new SqlCommand(createStatement, connection);
            cmd.ExecuteNonQuery();
        }

        private static int PrintHistory(Options options)
        {
            var runDate = DateTime.Now;
            Console.WriteLine("Print history started at: {0}", runDate);

            var connection = new SqlConnection(BuildConnectionString(options));
            try
            {
                connection.Open();
                var history = GetHistory(connection, null);
                foreach (var migration in history)
                {
                    Console.WriteLine(migration.Date + " - " + migration.Name);
                }
                Console.WriteLine("Print History complete :)");
                return 0;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ResetColor();
                return -1;
            }
            finally
            {
                connection.Close();
            }
        }
        
        private static void MigrateTables(SqlConnection connection, SqlTransaction transaction, Options options, DateTime date)
        {
            var path = options.Migrations;
            Console.WriteLine("Looking for Migrations in: " + Path.GetFullPath(path));
            var runMigrations = GetHistory(connection, transaction).Select(x => x.Name);
            var files = new DirectoryInfo(path).GetFiles("*.sql").Select(x => x.Name).ToList();
            var newMigrations = files.Except(runMigrations).ToList();

            Console.WriteLine("Found {0} migration(s). {1} already run - {2} new", files.Count(), files.Count() - newMigrations.Count(), newMigrations.Count());

            if (options.ListMigrations)
            {
                foreach (var mig in newMigrations)
                {
                    Console.WriteLine(mig);
                }
                return;
            }
            
            Console.WriteLine("Running {0} migrations", newMigrations.Count());
            foreach (var mig in newMigrations)
            {
                Console.WriteLine("Running script: {0}", mig);

                var script = File.ReadAllText(Path.Combine(path, mig));
                var commands = script.Split(new[] { "GO\r\n", "GO ", "GO\t" }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var c in commands)
                {
                    var command = new SqlCommand(c, connection, transaction);
                    command.ExecuteNonQuery();
                }

                var cmdVersion =
                    new SqlCommand(string.Format("Insert Into _Version Select '{0}', '{1}'", mig, date),
                        connection, transaction);
                cmdVersion.ExecuteNonQuery();
            }
        }

        private static IEnumerable<Migration> GetHistory(SqlConnection connection, SqlTransaction trans)
        {
            var migrations = new List<Migration>();
            const string query = "Select Migration, RunDate From _Version";
            var cmd = new SqlCommand(query, connection, trans);
            var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                migrations.Add(new Migration()
                {
                    Name = reader.GetFieldValue<string>(0), 
                    Date = reader.GetFieldValue<DateTime>(1)
                });
            }
            reader.Close();
            reader.Dispose();
            return migrations;
        }

        private static void RemoveRoutines(SqlConnection connection, SqlTransaction trans, DateTime runDate)
        {
            const string rawSqlDropScript = @"
                                                DECLARE @statement NVARCHAR(max)
                                                DECLARE @n CHAR(1)
                                                SET @n = CHAR(10) --space

                                                SELECT @statement = isnull( @statement + @n, '' ) +
                                                'DROP {0} [' + schema_name(so.schema_id) + '].[' + name + ']'
                                                FROM sys.objects so
                                                where so.[type] in ({1})

                                                EXEC sp_executesql @statement
                                                ";

            Console.WriteLine("Removing Routines");
            var dropViewsScript = string.Format(rawSqlDropScript, "VIEW", "'V'");
            var dropFuncScript = string.Format(rawSqlDropScript, "FUNCTION", "'FN', 'IF', 'TF'");
            var dropProcScript = string.Format(rawSqlDropScript, "PROCEDURE", "'P'");

            new SqlCommand(dropViewsScript, connection, trans).ExecuteNonQuery();
            new SqlCommand(dropFuncScript, connection, trans).ExecuteNonQuery();
            new SqlCommand(dropProcScript, connection, trans).ExecuteNonQuery();
        }

        private static void CreateRoutines(SqlConnection connection, SqlTransaction trans, Options options)
        {
            var path = options.Routines;
            var dependenciePath = Path.Combine(path, "dependencies.txt");
            var files = File.Exists(dependenciePath) 
                ? File.ReadAllLines(dependenciePath) 
                : new DirectoryInfo(path).GetFiles("*.sql").Select(x => x.Name);

            Console.WriteLine("Found {0} routines", files.Count());
            foreach (var file in files)
            {
                if (options.Verbose)
                    Console.WriteLine("Creating Routine: {0}", file);    

                var commands = File.ReadAllText(Path.Combine(path, file)).Split(new[] { "GO\r\n", "GO ", "GO\t" }, StringSplitOptions.RemoveEmptyEntries);

                foreach (var c in commands)
                {
                    var command = new SqlCommand(c, connection, trans);
                    command.ExecuteNonQuery();
                }
            }
        }

        private static string BuildConnectionString(Options options)
        {
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = options.Server,
                InitialCatalog = options.Database
            };

            if (options.IntegratedSecurity)
                builder.IntegratedSecurity = true;
            else
            {
                builder.UserID = options.User;
                builder.Password = options.Password;
            }

            return builder.ConnectionString;
        }

        private static string BuildMaserDbConnectionString(Options options)
        {
            var originalDbName = options.Database;
            options.Database = "master";
            var connectionResult =  BuildConnectionString(options);
            options.Database = originalDbName;
            return connectionResult;
        }
    }

    internal class Migration
    {
        public string Name { get; set; }
        public DateTime Date { get; set; }
    }
}
