using System;
using System.Data;
using System.IO;
using System.Text;
using System.Collections.Generic;

namespace DataSalesLibrary.DatabasePG
{
	/// <summary>
	/// Helper class for manipulating a DataTable
	/// </summary>
	class DataTableHelper
	{
		private DataTable m_Data;
		private bool m_bRestrictColumns;
		private string[] m_Columns;

		public DataTableHelper(DataTable dt)
		{
			m_Data = dt;
			m_bRestrictColumns = false;
		}

		public void useColumns(string[] useColumns)
		{
			m_Columns = useColumns;
			m_bRestrictColumns = true;
		}

		private bool useColumn(String columnName)
		{
			if (!m_bRestrictColumns) {
				return true;
			}
			else {
				for (int i = 0; i < m_Columns.Length; i++) {
					if (m_Columns[i] == columnName) {
						return true;
					}
				}
				return false;
			}
		}

		public void ProduceCSV(TextWriter httpStream, bool WriteHeader, bool ForceQuotes, Dictionary<String, String> ColumnAlias)
		{
            int iRowCount = m_Data.Rows.Count;
            int iColCount = m_Data.Columns.Count;

            StringBuilder sbLine = new StringBuilder();
            if (WriteHeader)
            {
                for (int i = 0; i < m_Data.Columns.Count; i++)
                {
                   // if (useColumn(m_Data.Columns[i].ColumnName))
                    {
                        string HeaderText = m_Data.Columns[i].ColumnName;
                        if (ColumnAlias != null)
                        {
                            ColumnAlias.TryGetValue(m_Data.Columns[i].ColumnName, out HeaderText);
                        }
                        sbLine.Append(((sbLine.Length > 0) ? "," : ""));
                        sbLine.Append(GetWriteableValue(HeaderText, ForceQuotes));
                    }
                }
                httpStream.WriteLine(sbLine.ToString());
            }else{
                //If no header and no rows, return a space to avoid XML parse error
                if(iRowCount == 0){
                    httpStream.Write(" ");
                }
            }

            for (int j = 0; j < iRowCount; j++)
            {
                sbLine.Length = 0;
                for (int i = 0; i < iColCount; i++)
                {
                    if (useColumn(m_Data.Columns[i].ColumnName))
                    {
                        sbLine.Append(((i > 0) ? "," : ""));
                        object o = m_Data.Rows[j][i];
                        sbLine.Append(GetWriteableValue(o, ForceQuotes));
                    }
                }
                httpStream.WriteLine(sbLine.ToString());
            }
		}

		public void ProduceCSV(StreamWriter file, bool WriteHeader, bool ForceQuotes)
		{
            StringBuilder sbLine = new StringBuilder();
            if (WriteHeader)
            {
				for (int i = 0; i < m_Data.Columns.Count; i++) {
					if (useColumn(m_Data.Columns[i].ColumnName)) {
						sbLine.Append(((sbLine.Length>0) ? "," : ""));
						sbLine.Append(GetWriteableValue(m_Data.Columns[i].ColumnName, ForceQuotes));
					}
				}
				file.WriteLine(sbLine.ToString());
			}

			for (int j = 0; j < m_Data.Rows.Count; j++) {
                sbLine.Length = 0;
				for (int i = 0; i < m_Data.Columns.Count; i++) {
					if (useColumn(m_Data.Columns[i].ColumnName)) {
                        sbLine.Append(((i > 0) ? "," : ""));
						object o = m_Data.Rows[j][i];
						sbLine.Append(GetWriteableValue(o, ForceQuotes));
					}
				}
				file.WriteLine(sbLine.ToString());
			}
		}

		private string GetWriteableValue(Object o, bool ForceQuotes)
		{
            if (o == null || o == Convert.DBNull)
            {
                return "";
            }
            else
            {
                String s = o.ToString();
                //s = s.Replace("\n", " ");
                s = s.Replace("\r", " ");
                s = s.Replace(",", "");

                if ((s.IndexOf(",") == -1) && !ForceQuotes)
                {
                    return s;
                }
                else
                {
                    return "\"" + s + "\"";
                }
            }
		}
	}
}
