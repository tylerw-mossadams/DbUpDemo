using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using NLog;

namespace DbUpDemo
{
    class Program
    {
        private static Logger _Logger = LogManager.GetCurrentClassLogger();

        static int Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();

			var Migrator = new DatabaseMigrator(configuration);
			
			foreach (string arg in args)
			{
				int argEnd = arg.IndexOf(":");
				if (argEnd == -1) throw new Exception("Unrecognizable argument supplied");
				if (argEnd == arg.Length) throw new Exception("No parmeter for argument was supplied");
				string argValue = arg.Substring(argEnd + 1);

				switch (arg.Substring(0, argEnd))
				{
					case "db":
						Migrator.SetConnection(argValue);
						break;

					case "v":
						Migrator.BuildVersion = argValue;
						break;
				}
			}

			int Result = Migrator.MigrateDatabase();
			return Result;

		}
    }
}
