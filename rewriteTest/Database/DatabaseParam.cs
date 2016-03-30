/*
* TITLE:        DatabaseParam
* DESCRIPTION:  Parameter class for passing into some of the Database query methods
* AUTHOR :      wrightc, Forth Valley GIS Ltd
* CREATED :     8/2/2010 12:14:10 PM
* DISCLAIMER:   The following disclaimer applies to all code unless otherwise stated.
* All intellectual property rights, including copyright, in this software code are owned by Forth Valley GIS Limited.  
* Your use of this software code is subject to all of the terms of the relevant licence granted by Forth Valley GIS Limited
*/
using System;
using System.Data;
using System.Configuration;
using System.Linq;
using Npgsql;


namespace DataSalesLibrary.DatabasePG
{
    /// <summary>
    /// Public class used for passing SQL query parameter info to functions in the Database class.
    /// Used by variants of RunQuery, RunQueryGetId, RunQueryNoData
    /// </summary>
    class DatabaseParam
    {
        private string m_sParamName;
        private object m_oParamValue;
        private int m_iParamSize;
        private int m_tParamType;
        private ParameterDirection m_ParamDirection;

        public DatabaseParam()
        {
            m_sParamName = "";
            m_oParamValue = "";
            m_iParamSize = 0;
            m_tParamType = (int)DbType.String;
            m_ParamDirection = ParameterDirection.Input;

        }

        public DatabaseParam(string sParamName, object oParamValue, System.Data.DbType tParamType)
        {
            m_sParamName = sParamName;
            m_oParamValue = oParamValue;
            m_iParamSize = 0;
            m_tParamType = (int)tParamType;
            m_ParamDirection = ParameterDirection.Input;
        }
        public DatabaseParam(string sParamName, object oParamValue, int iParamSize, System.Data.DbType tParamType)
        {
            m_sParamName = sParamName;
            m_oParamValue = oParamValue;
            m_iParamSize = iParamSize;
            m_tParamType = (int)tParamType;
            m_ParamDirection = ParameterDirection.Input;
        }
        public DatabaseParam(string sParamName, object oParamValue, int iParamSize, System.Data.DbType tParamType, ParameterDirection dirParamDirection)
        {
            m_sParamName = sParamName;
            m_oParamValue = oParamValue;
            m_iParamSize = iParamSize;
            m_tParamType = (int)tParamType;
            m_ParamDirection = dirParamDirection;
        }

        /// <summary>
        /// Name of database parameter as used in the SQL statement
        /// </summary>
        public string ParamName
        {
            get { return m_sParamName; }
            set { m_sParamName = value; }
        }

        /// <summary>
        /// Value of database parameter as used in the SQL statement
        /// </summary>
        public object ParamValue
        {
            get { return m_oParamValue; }
            set { m_oParamValue = value; }
        }

        /// <summary>
        /// Size of database parameter as used in the SQL statement.  Defaults to 0 if not specified.
        /// </summary>
        public int ParamSize
        {
            get { return m_iParamSize; }
            set { m_iParamSize = (int)value; }
        }

        /// <summary>
        /// Type of database parameter value as generic DbType
        /// </summary>
        public DbType ParamType
        {
            get { return (DbType)m_tParamType; }
            set { m_tParamType = (int)value; }
        }

        /// <summary>
        /// Direction of database parameter as generic ParameterDirection enum
        /// </summary>
        public ParameterDirection ParamDirection
        {
            get { return m_ParamDirection; }
            set { m_ParamDirection = (ParameterDirection)value; }
        }

        /// <summary>
        /// Returns an Postgres Parameter object with the given parameter values
        /// </summary>
        /// <returns>NpgsqlParameter</returns>
        public NpgsqlParameter GetDatabaseParam()
        {
            NpgsqlParameter newParam = new NpgsqlParameter(m_sParamName, m_oParamValue);
            newParam.DbType = (DbType)m_tParamType;
            newParam.Direction = m_ParamDirection;
            newParam.Size = m_iParamSize;

            //Modify value to null if empty value passed in and type is not string or xml
            if (m_oParamValue.ToString().Length == 0 && (DbType)m_tParamType != DbType.String && (DbType)m_tParamType != DbType.Xml)
            {
                newParam.Value = DBNull.Value;
            }

            return newParam;
        }
    }
}