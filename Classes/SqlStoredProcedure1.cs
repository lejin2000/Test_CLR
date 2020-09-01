using System; // contains types (String / Int64 / Guid), Exception, and ArgumentException
using System.Data; // contains the CommandType, ConnectionState, SqlDbType, and ParameterDirection enums
using System.Data.SqlClient; // contains SqlConnection, SqlCommand, SqlParameter, and SqlDataReader
using System.Data.SqlTypes; // contains the Sql* types for input / output params
using System.Text; // contains StringBuilder
using Microsoft.SqlServer.Server; // contains SqlFacet(Attribute), SqlContext, (System)DataAccessKind

public class Intro
{
	[Microsoft.SqlServer.Server.SqlProcedure(Name = "StairwayToSQLCLR_02_TestProc")]
	public static void TestProc([SqlFacet(MaxSize = 1000)] SqlString Query,
		SqlBoolean PrintFieldCount)
	{
		if (Query.IsNull || Query.Value.Trim() == string.Empty)
		{
			return;
		}


		SqlDataReader dataReader = null;
		SqlConnection sqlConnection = new SqlConnection("context connection = true");

		try
		{
			SqlCommand sqlCommand = new SqlCommand(Query.Value, sqlConnection);
			sqlCommand.CommandType = CommandType.Text;

			sqlConnection.Open(); // establish the connection

			dataReader = sqlCommand.ExecuteReader(); // run the query

			if (PrintFieldCount.IsTrue)
			{
				SqlContext.Pipe.Send("Number of fields: " + dataReader.FieldCount.ToString());
			}

			SqlContext.Pipe.Send(dataReader);
		}
		catch (Exception ex)
		{
			SqlContext.Pipe.Send("Error caught!");
			throw;
		}
		finally
		{
			// make sure to clean up external resources!
			SqlContext.Pipe.Send("Cleaning up...");

			if (dataReader != null && !dataReader.IsClosed)
			{
				dataReader.Close();
			}

			if (sqlConnection.State != ConnectionState.Closed)
			{
				sqlConnection.Close();
			}
		}

		SqlContext.Pipe.Send("Ending...");

		return;
	}


	[Microsoft.SqlServer.Server.SqlFunction(Name = "StairwayToSQLCLR_02_TestFunc",
		IsDeterministic = false, IsPrecise = true,
		DataAccess = DataAccessKind.Read, SystemDataAccess = SystemDataAccessKind.None)]
	public static SqlInt64 TestFunc([SqlFacet(MaxSize = 300)] SqlString TableName,
		SqlByte CountMethod, SqlBoolean InsertRow)
	{
		if (CountMethod.Value < 1 || CountMethod.Value > 10) // intentional bug to test RAISERROR
		{
			throw new ArgumentException("\n\nInvalid @CountMethod value: "
				+ CountMethod.Value
				+ "\n\nValid @CountMethod values are between 1 and 9.\n\n");
		}

		SqlInt64 numRows = -1;
		StringBuilder theQuery = new StringBuilder(420);
		SqlConnection sqlConnection = new SqlConnection("context connection = true");

		try
		{
			SqlCommand sqlCommand = new SqlCommand();
			sqlCommand.Connection = sqlConnection;

			switch (CountMethod.Value)
			{
				case 1:
					// Ideally, check via system table
					theQuery.AppendLine("SELECT SUM([rows]) AS [TotalRows]");
					theQuery.AppendLine("FROM sys.partitions sp");
					theQuery.Append("WHERE sp.[object_id] = OBJECT_ID('");
					theQuery.Append(TableName.Value.Trim());
					theQuery.AppendLine("')");
					theQuery.AppendLine("AND sp.index_id < 2");

					sqlCommand.CommandText = theQuery.ToString();
					sqlCommand.CommandType = CommandType.Text;
					break;
				case 2:
					// If system table no worky-worky, suck it up and 
					// check via the table itself
					theQuery.AppendLine("SELECT COUNT_BIG(*) AS [TotalRows]");
					theQuery.Append("FROM ");
					theQuery.Append(TableName.Value);
					theQuery.AppendLine(" WITH (NOLOCK)");

					sqlCommand.CommandText = theQuery.ToString();
					sqlCommand.CommandType = CommandType.Text;
					break;
				default:
					theQuery.Append("dbo.StairwayToSQLCLR_02_GetNumRows");

					sqlCommand.CommandText = theQuery.ToString();
					sqlCommand.CommandType = CommandType.StoredProcedure;

					SqlParameter paramTableName =
						sqlCommand.Parameters.Add("@TableName", SqlDbType.NVarChar, 128);
					paramTableName.Direction = ParameterDirection.Input;
					paramTableName.Value = TableName.Value;

					SqlParameter paramCountMethod =
						sqlCommand.Parameters.Add("@CountMethod", SqlDbType.TinyInt);
					paramCountMethod.Direction = ParameterDirection.Input;
					paramCountMethod.Value = CountMethod.Value;
					break;
			}

			sqlConnection.Open();

			numRows = (Int64)sqlCommand.ExecuteScalar();
		}
		catch (Exception ex)
		{
			throw new Exception("\n\n" + ex.Message +
				"\n\nQuery:\n" + theQuery.ToString() + "\n\n");
		}
		finally
		{
			// make sure to clean up external resources!
			if (sqlConnection.State != ConnectionState.Closed)
			{
				sqlConnection.Close();
			}
		}



		if (InsertRow.IsTrue)
		{
			sqlConnection = new SqlConnection(
				"server=localhost;trusted_connection=true;initial catalog=StairwayToSQLCLR;");

			try
			{
				theQuery.Length = 0; // clear out the StringBuilder

				theQuery.AppendLine("INSERT INTO dbo.StairwayToSQLCLR_02 ");
				theQuery.Append("(ID, InsertTime) VALUES ('");
				theQuery.Append(Guid.NewGuid());
				theQuery.Append("', '");
				theQuery.Append(System.DateTime.Now.ToShortDateString());
				theQuery.Append(" ");
				theQuery.Append(System.DateTime.Now.ToLongTimeString());
				theQuery.AppendLine("')");

				SqlCommand sqlCommand =
					new SqlCommand(theQuery.ToString(), sqlConnection);
				sqlCommand.CommandType = CommandType.Text;

				sqlConnection.Open();

				sqlCommand.ExecuteNonQuery();
			}
			catch (System.Security.SecurityException ex)
			{
				// If this is a SqlClientPermission error, give the user some useful info
				if (ex.PermissionType.FullName == "System.Data.SqlClient.SqlClientPermission")
				{
					throw new System.Security.SecurityException(
						  "\n\nSqlClientPermission exception.\n\nPlease run the following:"
						+ "\n\n\tALTER DATABASE [StairwayToSQLCLR] SET TRUSTWORTHY ON;"
						+ "\n\tALTER ASSEMBLY [StairwayToSQLCLR-02-Example] "
						+ "WITH PERMISSION_SET = EXTERNAL_ACCESS;\n\n\n");
				}

				throw;
			}
			catch (Exception ex)
			{
				throw new Exception("\n\n" + ex.Message +
					"\n\nQuery:\n" + theQuery.ToString() + "\n\n");
			}
			finally
			{
				// make sure to clean up external resources!
				if (sqlConnection.State != ConnectionState.Closed)
				{
					sqlConnection.Close();
				}
			}
		}

		return numRows;
	}

}
