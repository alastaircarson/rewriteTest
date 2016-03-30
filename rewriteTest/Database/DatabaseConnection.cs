using System;

namespace DataSalesLibrary.DatabasePG
{
    /// <summary>
    /// Summary description for DatabaseConnection
    /// </summary>
    public class DatabaseConnection
    {
        protected String m_sExpDBUser;
        protected String m_sExpDBPwd;
        protected String m_sExpDBServer;
        protected String m_sExpDBPort;
        protected String m_sExpDatabase;

        // Property Access Methods
        public String DBUser
        {
            get { return m_sExpDBUser; }
        }

        public String DBPwd
        {
            get { return m_sExpDBPwd; }
       }

        public String DBServer
        {
            get { return m_sExpDBServer; }
        }

        public String DBPort
        {
            get { return m_sExpDBPort; }
        }

        public String DBDatabase
        {
            get { return m_sExpDatabase; }
            set { m_sExpDatabase = value; }
        }

        /// <summary>
        /// Constructor initialises database connection parameters using the default website 
        /// database connection string
        /// </summary>
        public DatabaseConnection()
        {
            string sDbConnection = Database.GetConnString();
            ParseConnectionString(sDbConnection);

        }
        
        /// <summary>
        /// Initialises database connection parameters using a provided database connection
        /// string.  Added for extendability
        /// </summary>
        /// <param name="sConnectionName">Database connection string in standard .NET format</param>
        public DatabaseConnection(string sConnectionString)
        {
			ParseConnectionString(sConnectionString);
        }

        /// <summary>
        /// Overrides the default connection settings for the site with those
        /// for a different named connection from web.config 
        /// </summary>
        /// <param name="dbConnectionName">Name of alternative connection in web.config as string</param>
        /*
         * public void ReadConnectionSettings(string sConnectionName)
        {
         }
         */

        /// <summary>
        /// Returns connection settings as connection string
        /// </summary>
        /// <returns>regular database connectionstring as string. Returns empty string if no details known</returns>
        public override string ToString()
        {
            string sConnString = "";

            if (m_sExpDatabase.Length > 0 && m_sExpDBUser.Length > 0)
            {
                sConnString = "Server=" + m_sExpDBServer +
                                ";Port=" + m_sExpDBPort +
                                ";User id=" + m_sExpDBUser +
                                ";Password=" + m_sExpDBPwd +
                                ";Database=" + m_sExpDatabase +
                                ";ConnectionLifeTime=60";
            }
            return sConnString;
        }

        /// <summary>
        /// Does the work of parsing a connection string and setting the class properties
        /// </summary>
        /// <param name="connectionString">Database connection string in standard .NET format</param>
        private void ParseConnectionString(string connectionString){
            //Parse string to get username, pwd, etc.
            string[] asParams = connectionString.Split(';');
            if (asParams.Length > 1)
            {
                for (int i = 0; i < asParams.Length; i++)
                {
                    string sParam = asParams[i];
                    string[] asParamElement = sParam.Split('=');

                    switch (asParamElement[0].ToUpper().Trim())
                    {
                        case "USER ID":
                            m_sExpDBUser = asParamElement[1].Trim();
                            break;
                        case "PASSWORD":
                            m_sExpDBPwd = asParamElement[1].Trim();
                            break;
                        case "DATA SOURCE":
                        case "SERVER":
                            m_sExpDBServer = asParamElement[1].Trim();
                            break;
                        case "PORT":
                            m_sExpDBPort = asParamElement[1].Trim();
                            break;
                        case "DATABASE":
                            m_sExpDatabase = asParamElement[1].Trim();
                            break;
                    }
                }
            }
        }
    }
}

