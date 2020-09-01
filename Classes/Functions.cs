using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;

namespace Test_CLR
{
    public class Functions
    {
        [Microsoft.SqlServer.Server.SqlFunction]
        public static SqlString FormatDateTime( SqlDateTime dateToFormat, SqlString format)
        {
            return dateToFormat.Value.ToString(format.Value);
        }

    }
}
