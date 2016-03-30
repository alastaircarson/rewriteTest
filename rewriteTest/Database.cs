using System;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.Diagnostics;
using System.Threading;
using System.Collections;
using System.Collections.Generic;
using Npgsql;


namespace TWDatabase
{
	/// <summary>
	/// class to allow access to the Configuration database
	/// </summary>
	class Database
	{
        //Data Type Constants
        //BINARY TYPES
        public const string BIT = "bit";
        public const string BIT_VARYING = "bit varying";
        public const string BYTEA = "bytea";
        public const string VARBIT = "varbit";
        public static ArrayList BINARY_TYPES = new ArrayList { BIT, BIT_VARYING, BYTEA, VARBIT };

        //BOOLEAN
        public const string BOOL = "bool";
        public const string BOOLEAN = "boolean";

        //CHARACTER
        public const string BPCHAR = "bpchar";
        public const string CHAR = "char";
        public const string CHARACTER = "character";
        public const string CHARACTER_VARYING = "character varying";
        public const string TEXT = "text";
        public const string VARCHAR = "varchar";
        public static ArrayList CHARACTER_TYPES = new ArrayList
        {
            BPCHAR,
            CHAR,
            CHARACTER,
            CHARACTER_VARYING, 
            TEXT,
            VARCHAR
        };

        //DATE_TIME
        public const string DATE = "date";
        public const string INTERVAL = "interval";
        public const string TIME = "time";
        public const string TIMESTAMP = "timestamp";
        public const string TIMESTAMPTZ = "timestamptz";
        public const string TIMETZ = "timetz";

        //GEOMETRIC
        public const string BOX = "box";
        public const string CIRCLE = "circle";
        public const string GEOMETRY = "geometry";
        public const string LINE = "line";
        public const string LSEG = "lseg";
        public const string PATH = "path";
        public const string POINT = "point";
        public const string POLYGON = "polygon";

        //MONETRY
        public const string MONEY = "money";

        //NETWORK
        public const string CIDR = "cidr";
        public const string INET = "inet";
        public const string MACADDR = "macaddr";

        //NUMERIC
        public const string _INT4 = "_int4";
        public const string BIGINT = "bigint";
        public const string BIGSERIAL = "bigserial";
        public const string DECIMAL = "decimal";
        public const string DOUBLE_PRECISION = "double precision";
        public const string FLOAT = "float";
        public const string FLOAT4 = "float4";
        public const string FLOAT8 = "float8";
        public const string INT = "int";
        public const string INT2 = "int2";
        public const string INT4 = "int4";
        public const string INT8 = "int8";
        public const string INTEGER = "integer";
        public const string NUMERIC = "numeric";
        public const string REAL = "real";
        public const string SERIAL = "serial";
        public const string SERIAL4 = "serial4";
        public const string SERIAL8 = "serial8";
        public const string SMALLINT = "smallint";
        public static ArrayList NUMERIC_TYPES = new ArrayList 
        { 
            _INT4,
            BIGINT,
            BIGSERIAL,
            DECIMAL,
            DOUBLE_PRECISION,
            FLOAT,
            FLOAT4,
            FLOAT8,
            INT,
            INT2,
            INT4,
            INT8,
            INTEGER,
            NUMERIC,
            REAL,
            SERIAL,
            SERIAL4,
            SERIAL8,
            SMALLINT
        };

        //SYSTEM
        public const string OID = "oid";
        public const string TSQUERY = "tsquery";
        public const string TSVECTOR = "tsvector";
        public const string TXID_SNAPSHOT = "txid_snapshot";
        public const string XID = "xid";

        //UUID
        public const string UUID = "uuid";

        //XML
        public const string XML = "xml";


        protected static bool m_bHasConnection = false;
        protected static String m_ConnectionString;             //Actual connection string retrieved from web.config
		protected static String m_LastError;
		//protected static Mutex m_Lock = new Mutex();


        /// <summary>
        /// Sets the static connection string.  This is typically called from the Global.asax file
        /// once the application has determined which customer site is being accessed.
        /// </summary>
        /// <param name="sConnectionName">Name of connection string in </param>
        public static void SetDBConnection(string sConnectionString)
        {
			m_ConnectionString = sConnectionString;
            m_bHasConnection = true;
        }

        
        /// <summary>
        /// Reports whether or not the static connection string variable has been set.  
        /// The connection string should be set by Global.asax when a session starts, but checking this
        /// first will save some time.
        /// </summary>
        /// <returns>True if a database connection string has been set; False otherwise</returns>
        public static bool HasConnectionString()
        {
            return m_bHasConnection;
        }


		/// <summary>
		/// Gets last database error
		/// </summary>
		/// <returns>Last error message as string</returns>
		public static String GetLastError()
		{
			return m_LastError;
		}

		/// <summary>
		/// Initializes database connection with default connection string for site
		/// </summary>
        /// <returns>Postgres connection as NpgsqlConnection</returns>
        public static NpgsqlConnection Init()
		{
			return Init(new DatabaseConnection());
		}

        /// <summary>
        /// Initializes database connection using the default connection string but specifying a different database
        /// </summary>
        /// <param name="DatabaseName">Database name to connect to</param>
        /// <returns>Postgres connection as NpgsqlConnection</returns>
        public static NpgsqlConnection Init(string sDatabaseName)
        {
            DatabaseConnection dcConnection = new DatabaseConnection();
            if (sDatabaseName.Length > 0)
            {
                //Overwrite default database name
                dcConnection.DBDatabase = sDatabaseName;
            }

            return Init(dcConnection);

        }
        
        /// <summary>
        /// Initializes database connection with a provided connection string
        /// </summary>
        /// <param name="dcConnection">Postgres connection details as DatabaseConnection object</param>
        /// <returns>Postgres connection as NpgsqlConnection</returns>
        public static NpgsqlConnection Init(DatabaseConnection dcConnection)
        {
            NpgsqlConnection connection = new NpgsqlConnection(dcConnection.ToString());
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                if (connection.State != ConnectionState.Closed)
                {
                    Close(connection);
                }
                connection = null;
                throw new DatabaseException("Error opening db connection: " + ex.Message);
            }

            return connection;
        }

		/// <summary>
		/// Closes the specified database connection
		/// </summary>
		/// <param name="connection">Database connection as NpgsqlConnection</param>
        public static void Close(NpgsqlConnection connection)
		{
			connection.Close();
			connection = null;
		}

		/// <summary>
		/// Runs a SQL query and returns results as a DataTable.  Suitable for Select queries
		/// </summary>
		/// <param name="strSQL">SQL query as string</param>
		/// <returns>Results as DataTable</returns>
		public static DataTable RunQuery(String strSQL)
		{
            return RunQuery(strSQL,null, null);
		}
        
        /// <summary>
        /// Runs a SQL query and returns results as a DataTable.  Suitable for Select queries
        /// </summary>
        /// <param name="strSQL">SQL query as string</param>
        /// <param name="aParams">Array of DatabaseParam objects containing parameter information in format
        ///     ("key",[DBFieldType sqltype,object sqlvalue])
        ///     If no params, pass in null;</param>
        /// <returns>Results as DataTable</returns>
        public static DataTable RunQuery(String strSQL, IEnumerable aParams)
        {
            return RunQuery(strSQL, null, aParams);
        }

		/// <summary>
		/// Runs a SQL query and returns results as a DataTable.  Suitable for Select queries
		/// </summary>
		/// <param name="strSQL">SQL query as string</param>
        /// <param name="sDBName">Name of database as string. Pass in null or empty string to use default.</param>
        /// <param name="aParams">Array of DatabaseParam objects containing parameter information in format
		///     ("key",[DBFieldType sqltype,object sqlvalue])
        ///     If no params, pass in null;</param>
		/// <returns>Results as DataTable</returns>
		public static DataTable RunQuery(String strSQL, String sDBName, IEnumerable aParams)
		{
            NpgsqlConnection connection;
            if (sDBName != null && sDBName.Length > 0)
            {
                connection = Init(sDBName);
            }
            else
            {
                connection = Init();
            }

            DataSet dsTemp = new DataSet();
            
            NpgsqlCommand ocCmd = new NpgsqlCommand(strSQL, connection);

            try
            {
                if (aParams != null)
                {
                    foreach (DatabaseParam dp in aParams)
                    {
                        ocCmd.Parameters.Add(dp.GetDatabaseParam());
                    }
                }

                NpgsqlDataAdapter daCmd = new NpgsqlDataAdapter(ocCmd);

                daCmd.Fill(dsTemp, "myQuery");
            }
            catch (Exception ex)
            {
                throw new DatabaseException(ex.Message,strSQL);
            }
            finally
            {
                ocCmd.Dispose();
                Close(connection);
            }
            //Return a copy so that not tied to original dataset
			return dsTemp.Tables[0].Copy();
		}

		/// <summary>
		/// Runs query to add a record then returns the ID of the record just added.
		/// This is used when a trigger is used to populate an ID number for this record.
		/// Works whether or not you use a sequence
		/// </summary>
		/// <param name="strSQL">SQL insert statement</param>
		/// <param name="strIDFieldName">Name of ID field. Query will return the value in this field for the row just added</param>
		/// <returns>ID of inserted row if successful; -1 otherwise.</returns>
		public static int RunQueryGetID(String strSQL, string sIDFieldName)
		{
            return RunQueryGetID(strSQL, sIDFieldName, null);
		}

		/// <summary>
		/// Runs query to add a record then returns the ID of the record just added.
		/// This is used when a trigger is used to populate an ID number for this record.
		/// Works whether or not you use a sequence
		/// This version is suitable for parameterised queries, improving data security
		/// </summary>
		/// <param name="strSQL">SQL insert statement</param>
		/// <param name="strIDFieldName">Name of ID field. Query will return the value in this field for the row just added</param>
		/// <param name="aParams">Array of DatabaseParam objects containing parameter information; Null if not used;
		/// <returns>ID of inserted row if successful; -1 otherwise.</returns>
        public static int RunQueryGetID(String strSQL, string sIDFieldName, IEnumerable aParams)
		{
			NpgsqlConnection connection = Init();
			int iOpID = -1;

			//Add returning <idfield> as newid; to end of query 
			strSQL += " returning " + sIDFieldName;

            try
            {
                NpgsqlCommand ocCmd = new NpgsqlCommand(strSQL, connection);

                NpgsqlParameter opNewId = new NpgsqlParameter("newid", DbType.Int32);
                opNewId.Direction = ParameterDirection.Output;		//Set param direction to output
                ocCmd.Parameters.Add(opNewId);						//Add the parameter to the command

                if (aParams != null)
                {
                    foreach (DatabaseParam dp in aParams)
                    {
                        ocCmd.Parameters.Add(dp.GetDatabaseParam());
                    }
                }

                ocCmd.ExecuteNonQuery();

                iOpID = (int)ocCmd.Parameters[0].Value;

                ocCmd.Dispose();
            }
            catch (Exception ex)
            {
                iOpID = -1;
                throw new DatabaseException(ex.Message,strSQL);
            }
            finally
            {
                Close(connection);
            }
            
            return iOpID;
        }

		/// <summary>
		/// Runs query to add a record then returns the ID of the record just added.
		/// This is used when a trigger is used to populate an ID number for this record.
		/// NOTE: only works if using a sequence to maintain the id, not if just adding 1 to max(id)
		/// </summary>
		/// <param name="strSQL">SQL insert statement</param>
		/// <returns>ID of inserted row if successful; -1 otherwise.</returns>
		public static int RunQueryGetID(String strSQL)
		{
			return RunQueryGetID(strSQL, new DatabaseParam[]{});
		}

		/// <summary>
		/// Runs query to add a record then returns the ID of the record just added.
		/// This is used when a trigger is used to populate an ID number for this record.
		/// NOTE: only works if using a sequence to maintain the id, not if just adding 1 to max(id)
		/// </summary>
		/// <param name="strSQL">SQL insert statement</param>
		/// <returns>ID of inserted row if successful; -1 otherwise.</returns>
		public static int RunQueryGetID(String strSQL, IEnumerable aParams)
		{
			NpgsqlConnection connection = Init();
			int iOpID = -1;

			//Add SELECT lastval() as ID; to end of query 
            if (strSQL.IndexOf("lastval()") == -1)
            {
				strSQL += "; SELECT lastval() as ID;";
			}

            try
            {
				NpgsqlCommand ocCmd = new NpgsqlCommand(strSQL, connection);
                DataSet dsTemp = new DataSet();

				if(aParams != null)
				{
					foreach(DatabaseParam dp in aParams)
					{
						ocCmd.Parameters.Add(dp.GetDatabaseParam());
					}
				}

				NpgsqlDataAdapter daCmd = new NpgsqlDataAdapter(ocCmd);
				daCmd.Fill(dsTemp, "myQuery");
                DataTable dtID = dsTemp.Tables[0];

                iOpID = (int)Convert.ToInt32(Database.GetQueryValue(dtID, 0, "ID"));
            }
            catch (Exception ex)
            {
                iOpID = -1;
                throw new DatabaseException(ex.Message);
            }
            finally
            {
                Close(connection);
            }
			
            return iOpID;
		}

		/// <summary>
		/// Runs a SQL query string without returning any results from the query.  
		/// Good for Inserts, updates and Deletes
		/// </summary>
		/// <param name="strSQL">SQL statement as string</param>
		/// <returns>True if successful; False otherwise, and m_LastError set to error message</returns>
		public static bool RunQueryNoData(String strSQL)
		{
            return RunQueryNoData(strSQL,"", null,null);
		}

        //Wrapper for backwards compatibility following addition of Database parameter
        public static bool RunQueryNoData(String strSQL, IEnumerable aParams)
        {
            return RunQueryNoData(strSQL, "", aParams, null);
        }
        
        /// <summary>
		/// Runs a SQL query string without returning any results from the query.
		/// Good for Inserts, updates and Deletes.
		/// This version is suitable for parameterised queries, improving data security
		/// </summary>
		/// <param name="strSQL">SQL statement as string, with parameters defined in query</param>
        /// <param name="sDBName">Name of database as string; empty string to use default database</param>
        /// <param name="aParams">Collection of DatabaseParam objects containing sql param info as Enumerable 
        /// Null if not required
		/// </param>
		/// <returns>True if successful; False otherwise, and m_LastError set to error message</returns>
        public static bool RunQueryNoData(String strSQL, String sDBName, IEnumerable aParams)
        {
            return RunQueryNoData(strSQL,sDBName, aParams, null);
        }

        //Wrapper for backwards compatibility following addition of Database parameter
        public static bool RunQueryNoData(String strSQL, IEnumerable aParams, DatabaseConnection conn)
        {
            return RunQueryNoData(strSQL, "", aParams, conn);
        }
           
        /// <summary>
		/// Runs a SQL query string without returning any results from the query.
		/// Good for Inserts, updates and Deletes.
		/// This version is suitable for parameterised queries, improving data security
		/// </summary>
		/// <param name="strSQL">SQL statement as string, with parameters defined in query</param>
        /// <param name="sDBName">Name of database as string; empty string to use default database</param>
		/// <param name="aParams">Collection of DatabaseParam objects containing sql param info as Enumerable; (Array or List<>) 
        /// Null if not required
		/// </param>
        /// <param name="conn">Database connection as NpgsqlConnection. If Null, uses default connection for site
        /// </param>
		/// <returns>True if successful; False otherwise, and m_LastError set to error message</returns>
        public static bool RunQueryNoData(String strSQL, String sDBName, IEnumerable aParams, DatabaseConnection conn)
		{
            NpgsqlConnection connection;
            if (conn != null)
            {
                connection = Init(conn);
            }
            else if (sDBName != null && sDBName.Length > 0)
            {
                connection = Init(sDBName);
            }
            else
            {
                connection = Init();
            }

            try
            {
                NpgsqlCommand dcCmd = connection.CreateCommand();
                dcCmd.CommandText = strSQL;

                if (aParams != null)
                {
                    foreach (DatabaseParam dp in aParams)
                    {
                        dcCmd.Parameters.Add(dp.GetDatabaseParam());
                    }
                }

                dcCmd.ExecuteNonQuery();
                return true;
            }
            catch (NpgsqlException e)
            {
                m_LastError = e.Message;
                throw new DatabaseException(e.Message);
            }
            finally
            {
                Close(connection);
            }
		}

		/// <summary>
		/// Returns value from the specified column and row from a datatable of data.  Values 
		/// returned as strings.
		/// </summary>
		/// <param name="table">Data to return value from as DataTable </param>
		/// <param name="RowNo">Row number as integer</param>
		/// <param name="columnName">Name of column as string</param>
		/// <returns>value as string</returns>
		public static String GetQueryValue(DataTable table, int RowNo, String columnName)
		{
			int iColumn = table.Columns.IndexOf(columnName.ToLower());
            if (iColumn >= 0)
            {
                return table.Rows[RowNo][iColumn].ToString();
            }
            else
            {
                return "";
            }
		}

		/// <summary>
		/// Returns value from the specified column and row from a datatable of data.  Values 
		/// returned as strings.
		/// -Formats datetime objects into standard strings if required.
		/// </summary>
		/// <param name="table">Data to return value from as DataTable </param>
		/// <param name="RowNo">Row number as integer</param>
		/// <param name="columnName">Name of column as string</param>
		/// <param name="rtntype">"shortdate" will return datetime values in the shortdate format dd/mm/yy
		///   "longdate" will return datetime values in the longdate format</param>
		/// <returns>value as string, with dates as normal date string format.</returns>
		public static String GetQueryValue(DataTable table, int RowNo, String columnName, String rtntype)
		{

			int iColumn = table.Columns.IndexOf(columnName.ToLower());
            if (table.Columns[iColumn].DataType.Equals(System.Type.GetType("System.DateTime")))
            {
                if (table.Rows[RowNo][iColumn].ToString().Length > 0)
                {
					DateTime dt = Convert.ToDateTime(table.Rows[RowNo][iColumn]);
                    if (rtntype.Equals("shortdate"))
                    {
						return dt.Date.ToShortDateString();
					}
                    else if (rtntype.Equals("longdate"))
                    {
						return dt.Date.ToLongDateString();
					}
                    else
                    {
						return dt.Date.ToString();
					}
				}
                else
                {
					return "";
				}
			}
            else
            {
				return table.Rows[RowNo][iColumn].ToString();
			}
		}

		/// <summary>
		/// Returns values from a column of a datatable as a comma-separated list
		/// </summary>
		/// <param name="table">Table of data as DataTable</param>
		/// <param name="columnName">Name of the column to output as list, as string</param>
		/// <returns>List of values as string</returns>
		public static String GetValueList(DataTable table, String columnName)
		{
			StringBuilder sbList = new StringBuilder();
			int iColumn = table.Columns.IndexOf(columnName.ToLower());
            for (int i = 0; i < table.Rows.Count; i++)
            {
				if (i > 0) sbList.Append(",");
				sbList.Append(table.Rows[i][iColumn].ToString());
			}
			return sbList.ToString();
		}

		/// <summary>
		/// Returns values from a column of a datatable as an array of objects.
		/// This is often easier to handle programatically than GetValueList above
		/// </summary>
		/// <param name="table">Table of data as DataTable</param>
		/// <param name="columnName">Name of the column to output as list, as string</param>
		/// <returns>List of values as array of Objects</returns>
		public static Object[] GetValueArray(DataTable table, String columnName)
		{
			List<Object> sbList = new List<Object>();
			int iColumn = table.Columns.IndexOf(columnName.ToLower());
            for (int i = 0; i < table.Rows.Count; i++)
            {
				sbList.Add(table.Rows[i][iColumn]);
			}
			return sbList.ToArray();
		}

		/// <summary>
		/// Returns the database connection string
		/// </summary>
		/// <returns>Connection string as string</returns>
		public static String GetConnString()
		{
			return m_ConnectionString;
		}

        /// <summary>
        /// Returns the databaseconnection object containing individual parameters
        /// for the default site connection
        /// </summary>
        /// <returns>Connection details as DatabaseConnection</returns>
        public static DatabaseConnection GetConnection()
        {
            return new DatabaseConnection();
        }
        
        /// <summary>
        /// Returns the databaseconnection object containing individual parameters
        /// </summary>
        /// <param name="sConnectionName">Name of connection in web.config as string</param>
        /// <returns>Connection string as string</returns>
        public static DatabaseConnection GetConnection(string sConnectionName)
        {
            DatabaseConnection dbConnection = new DatabaseConnection(sConnectionName);
            return dbConnection;
        }

        /// <summary>
        /// Returns the NpgsqlConnection for the DataManager
        /// </summary>
        /// <returns>Connection as NpgsqlConnection</returns>
        public static DatabaseConnection GetDataManagerConnection()
        {
            return new DatabaseConnection("DataManager");
        }


		/// <summary>
		/// Check that specified table exists
		/// </summary>
		/// <param name="strOwner">Database schema name as string</param>
		/// <param name="strTable">Database table name as string</param>
		/// <returns>True if table exists; False otherwise</returns>
		public static bool TableExists(String strOwner, String strTable)
		{
			String sqlTable = "SELECT * FROM pg_tables WHERE schemaname = '" + strOwner.ToLower() + "' AND tablename = '" + strTable.ToLower() + "'";
			DataTable dtTable = RunQuery(sqlTable);
			return (dtTable.Rows.Count == 1);
		}

		/// <summary>
		/// Check that specified geometry column exists and is registered properly
		/// </summary>
		/// <param name="strOwner">Database schema name as string</param>
		/// <param name="strTable">Database table name as string</param>
		/// <param name="strGeometry">Geometry column name as string</param>
		/// <returns>True if OK; False otherwise</returns>
		public static bool GeometryExists(String strOwner, String strTable, String strGeometry)
		{
			String sqlTable = "SELECT SRID FROM public.geometry_columns WHERE f_table_schema = '" + strOwner + "' AND f_table_name = '" + strTable + "' AND f_geometry_column = '" + strGeometry + "'";
			DataTable dtTable = RunQuery(sqlTable);
			return (dtTable.Rows.Count == 1);
		}

		/// <summary>
		/// Check a column exists in a specified database table, optionally checking type
		/// </summary>
		/// <param name="strOwner">Database schema name as string</param>
		/// <param name="strTable">Database table name as string</param>
		/// <param name="strColumnName">Column name as string</param>
		/// <param name="strType">Data Type as string. This is an Oracle data type name. Use empty string to ignore type</param>
		/// <returns>True if column exists (and is of the correct type); False otherwise.</returns>
		public static bool ColumnExists(String strOwner, String strTable, String strColumnName, String strType)
		{
			String sqlColumn =
				"SELECT fieldtype FROM v_table_info WHERE schemaname = '" + strOwner + "' " +
				"AND tablename = '" + strTable + "' " +
				"AND fieldname = '" + strColumnName + "' ";
			DataTable dtColumn = RunQuery(sqlColumn);
			return (dtColumn.Rows.Count == 1);
		}



        /// <summary>
		/// Returns Column Names and Types of a specified database table
		/// </summary>
		/// <param name="strOwner">Database schema name as string</param>
		/// <param name="strTable">Database table name as string</param>
		/// <returns>List of column fieldnames as string</returns>
		public static DataTable GetColumnList(String strOwner, String strTable)
		{
            DataTable dtColumn;
			String sqlColumn =
				"SELECT fieldname FROM v_table_info " +
                "WHERE schemaname = '" + strOwner + "' " +
				"AND tablename = '" + strTable + "';" ;
			return dtColumn = RunQuery(sqlColumn);
		}


		/// <summary>
        /// Check a column exists in a specified database table, returning the postgis datatype
        /// </summary>
        /// <param name="strOwner">Database schema name as string</param>
        /// <param name="strTable">Database table name as string</param>
        /// <param name="strColumnName">Column name as string</param>
        /// <param name="dataType">Data Type as string. This is an postgres data type name.</param>
        /// <returns>True if column exists (and is of the correct type); False otherwise. Sets dataType by reference to contain postgis datatype </returns>
        public static bool GetColumnDataType(String strOwner, String strTable, String strColumnName, ref string dataType)
        {
            String sqlColumn =
                "SELECT fieldtype FROM v_table_info WHERE schemaname = '" + strOwner + "' " +
                "AND tablename = '" + strTable + "' " +
                "AND fieldname = '" + strColumnName + "' ";
            System.Diagnostics.Trace.Write("sqlColumn: " + sqlColumn);
            DataTable dtColumn = RunQuery(sqlColumn);
            if (dtColumn.Rows.Count == 1)
            {
                dataType = dtColumn.Rows[0]["fieldtype"].ToString();
                return true;
            }
            return false;
        }

        /// <summary>
		/// checks a SQL contruct is valid e.g. MapTip, etc..
		/// </summary>
		/// <param name="strOwner">Database schema name as string</param>
		/// <param name="strTable">Database table name as string</param>
		/// <param name="strConstruct">SQL extract to test as string</param>
		/// <param name="strError">Reference to string parameter that will hold error message. This is populated with any error message encountered.</param>
		/// <returns>True if valid; False otherwise - check strError in this case.</returns>
		public static bool SQLConstructValid(String strOwner, String strTable, String strConstruct, ref String strError)
		{
			bool bValid = true;
			String strSQL =
				"SELECT " + strConstruct + " AS Construct " +
				" FROM " + strOwner + "." + strTable +
				" limit 2";
			NpgsqlConnection connection = Init();
			NpgsqlDataAdapter daCmd = new NpgsqlDataAdapter(strSQL, connection);
			DataSet dsTemp = new DataSet();
            try
            {
                daCmd.Fill(dsTemp, "myQuery");
            }
            catch (NpgsqlException e)
            {
                bValid = false;
                strError = e.Message;
                Trace.WriteLine("Error running query: " + e.Message);
            }
            finally
            {
                Close(connection);
            }
			return bValid;
		}

		/// <summary>
		/// Cleans a string for use in a SQL query.  Replaces ";" with "", and "'" with "''"
		/// Also truncates string if longer than specified length.
		/// </summary>
		/// <param name="strIn">String value to check</param>
		/// <param name="length">Max length of string</param>
		/// <returns>Validated string.</returns>
		public static String CheckInput(String strIn, int length)
		{
			// replace 's to avoid Oracle insert errors
			// replace ;'s to avoid inputs causing command submissions
			// check length of input
			String strOut = strIn.Replace(";", "").Replace("'", "''");
            if (strOut.Length > length)
            {
				strOut = strOut.Substring(0, length);
			}
			return strOut;
		}

        /// <summary>
        /// Accepts field type held in mg_attributes table and 
        /// returns corresponding DBType enumeration
        /// </summary>
        /// <param name="sPGType">field type as string</param>
        /// <returns>Field type as DBType enumeration</returns>
        public static DbType PGTypeToDBType(string sPGType)
        {
            //not yet a complete list of possible pg types
            switch (sPGType.ToLower())
            {
                case "date":
                    return DbType.Date;
                case "timestamp":
                case "timestamptz":
                    return DbType.DateTime;
                case "text":
                case "varchar":
                    return DbType.String;
                case "bpchar":
                    return DbType.StringFixedLength;
                case "smallint":
                case "int2":
                    return DbType.Int16;
                case "oid":
                case "serial":
                case "serial4":
                case "int4":
                    return DbType.Int32;
                case "int8":
                    return DbType.Int64;
                case "real":
                case "float4":
                    return DbType.Decimal;
                case "float8":
                    return DbType.Double;
                case "decimal":
                case "numeric":
                    return DbType.VarNumeric;
                case "geometry":
                case "bytea":
                    return DbType.Binary;
                case "xml":
                    return DbType.Xml;
                default:
                    return DbType.String;
            }
        }

		#region Private Helper Functions



		#endregion
	}

    public class DatabaseException : Exception{
        public DatabaseException(string sErrorMessage):
            base("Error in db query: " + sErrorMessage) { }
        
        public DatabaseException(string sErrorMessage,string sqlQuery) :
            base("Error in db query: " + sErrorMessage + " - " + sqlQuery) { }
    }

}
