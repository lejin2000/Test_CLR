using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Test_CLR
{
    public class StoredProcedures
    {

		[Microsoft.SqlServer.Server.SqlProcedure]
		public static void ProcessSum()
		{
			decimal TotalRevenue = GetSum();
			InsertSum(TotalRevenue);
			SelectSum();
		}

		public static decimal   GetSum()
        {

			decimal TotalRevenue = 0;
			 SqlDataReader dataReader = null;
			SqlConnection sqlConnection = new SqlConnection("context connection = true");

			try
			{
				String Query;

				Query = "SELECT * FROM [SqlCLR].[dbo].[Total]";

				SqlCommand sqlCommand = new SqlCommand(Query, sqlConnection);
				sqlCommand.CommandType = CommandType.Text;

				sqlConnection.Open(); // establish the connection

				//dataReader = sqlCommand.ExecuteReader(); // run the query
				using (dataReader = sqlCommand.ExecuteReader())
				{
					while (dataReader.Read())
					{
						 TotalRevenue +=  (decimal)dataReader["TotalRevenue"];
					}
				}


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

			return TotalRevenue;
		}

		public static void InsertSum(Decimal TotalRevenue)
        {
			//SqlDataReader dataReader = null;
			SqlConnection sqlConnection = new SqlConnection("context connection = true");

			try
			{
				String Query;
				//Decimal TotalRevenue;
				//TotalRevenue = 100.11m;

				Query = "INSERT INTO [SqlCLR].[dbo].[Total] ([TotalRevenue]) VALUES  (" + TotalRevenue + ")";

				SqlCommand sqlCommand = new SqlCommand(Query, sqlConnection);
				sqlCommand.CommandType = CommandType.Text;

				sqlConnection.Open(); // establish the connection

				sqlCommand.ExecuteNonQuery(); // run the query

				 

				//SqlContext.Pipe.Send(dataReader);
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

				//if (dataReader != null && !dataReader.IsClosed)
				//{
				//	dataReader.Close();
				//}

				if (sqlConnection.State != ConnectionState.Closed)
				{
					sqlConnection.Close();
				}
			}

			SqlContext.Pipe.Send("Ending...");

			return;
		}

		public static void SelectSum()
        {
			SqlDataReader dataReader = null;
			SqlConnection sqlConnection = new SqlConnection("context connection = true");

			try
			{
				String Query; 

				Query = "SELECT * FROM [SqlCLR].[dbo].[Total]";

				SqlCommand sqlCommand = new SqlCommand(Query, sqlConnection);
				sqlCommand.CommandType = CommandType.Text;

				sqlConnection.Open(); // establish the connection

				dataReader = sqlCommand.ExecuteReader(); // run the query
				 

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

         
        [Microsoft.SqlServer.Server.SqlProcedure]
        public static void HelloWorld()
         {
                // Put your code here  
            SqlContext.Pipe.Send("This is my CLR SP test");
         }
        
    }
}
