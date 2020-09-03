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
        public static List<Total> TotalRevenueList { get; set; }

        [Microsoft.SqlServer.Server.SqlProcedure]
		public static void ProcessSum()
		{
			decimal TotalRevenue = GetSum();
			InsertSum(TotalRevenue);
			SelectSum();
		}

		[Microsoft.SqlServer.Server.SqlProcedure]
		public static void ReturnSum()
        {
			GetTotalAmountIntoList();
			ReturnSalesProcessedRecords(TotalRevenueList);
		}

		public static decimal   GetSum()
        {
			List<Total> _Total = new List<Total>();
			List<Total> _Total2 = new List<Total>();

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
						Total _TotalItem = new Total();

						//TotalRevenue =  (decimal)dataReader["TotalRevenue"] +  1;
						_TotalItem.TotalRevenue = Convert.ToDecimal(dataReader["TotalRevenue"]);
						_TotalItem.DatetimeStamp = Convert.ToDateTime(dataReader["DatetimeStamp"]);
						_Total.Add(_TotalItem);
					}
				}

				foreach (Total _TotalItem in _Total)
				{
					Total _TotalItem2 = new Total();
					_TotalItem2.TotalRevenue = _TotalItem.TotalRevenue;
					_TotalItem2.DatetimeStamp = _TotalItem.DatetimeStamp;
					_Total2.Add(_TotalItem2);
				}

				foreach (Total _TotalItem3 in _Total2)
				{
					TotalRevenue = _Total2.Count + Convert.ToDecimal(0.01);
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


		public static void GetTotalAmountIntoList()
		{
			List<Total> _Total = new List<Total>();
			List<Total> _Total2 = new List<Total>();
			 
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
						Total _TotalItem = new Total();
						_TotalItem.Id = Convert.ToInt32(dataReader["id"]);
						_TotalItem.TotalRevenue = Convert.ToDecimal(dataReader["TotalRevenue"]);
						_TotalItem.DatetimeStamp = Convert.ToDateTime(dataReader["DatetimeStamp"]);
						_Total.Add(_TotalItem);
					}
				}

				foreach (Total _TotalItem in _Total)
				{
					Total _TotalItem2 = new Total();
					_TotalItem2.Id = _TotalItem.Id;
					_TotalItem2.TotalRevenue = _TotalItem.TotalRevenue;
					_TotalItem2.DatetimeStamp = _TotalItem.DatetimeStamp;
					_Total2.Add(_TotalItem2);
				}

			 
				TotalRevenueList = _Total2;
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

		 
		}

		private static void ReturnSalesProcessedRecords(List<Total> _totalRevenueList)
		{

			SqlPipe pipe = SqlContext.Pipe;
			SqlMetaData[] cols = new SqlMetaData[3];
			cols[0] = new SqlMetaData("id", SqlDbType.Int);
			cols[1] = new SqlMetaData("TotalRevenue", SqlDbType.Decimal, 38, 4);
			cols[2] = new SqlMetaData("DatetimeStamp", SqlDbType.DateTime);
			 

			SqlDataRecord row = new SqlDataRecord(cols);
			pipe.SendResultsStart(row);

			//TODO: Sort the list once more time before projecting them into a table
			foreach (var _totalRevenueItem in _totalRevenueList)
			{
				row.SetInt32(0, _totalRevenueItem.Id);
				row.SetDecimal(1,  _totalRevenueItem.TotalRevenue);
				row.SetDateTime(2, _totalRevenueItem.DatetimeStamp);

				pipe.SendResultsRow(row);
			}
			pipe.SendResultsEnd();
		}

	}
}
