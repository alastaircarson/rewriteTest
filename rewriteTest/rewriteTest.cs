using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
//using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using Microsoft.Web.Iis.Rewrite;
using System.Configuration;
using TWDatabase;

namespace rewriteTest
{
	public class rewriteTest : IRewriteProvider
	{
		// Parameters
		protected String m_ConfigFile;
		protected String m_WMSUrl;
		protected String m_ErrorUrl;
		protected int m_TileSize;

		// Local Variables
		protected SiteConfigFile m_SiteConfig;
		protected CustomerConfigFile m_CustomerConfig;
		protected bool m_bInit = false;
		protected String m_Extent = "";
		protected String m_ConnectionString = "";
		protected int m_Count = 0;
		
		#region IRewriteProvider Members

		public void Initialize(IDictionary<string, string> settings, IRewriteContext rewriteContext)
		{
			String strTileSize;

			if (!settings.TryGetValue("Config", out m_ConfigFile) || string.IsNullOrEmpty(m_ConfigFile))
				throw new ArgumentException("Config provider setting is required and cannot be empty");

			if (!settings.TryGetValue("WMSUrl", out m_WMSUrl) || string.IsNullOrEmpty(m_WMSUrl))
				throw new ArgumentException("WMSUrl provider setting is required and cannot be empty");

			if (!settings.TryGetValue("ErrorUrl", out m_ErrorUrl) || string.IsNullOrEmpty(m_ErrorUrl))
				throw new ArgumentException("ErrorUrl provider setting is required and cannot be empty");

			if (settings.TryGetValue("TileSize", out strTileSize) && !string.IsNullOrEmpty(strTileSize) && Information.IsNumeric(strTileSize))
				m_TileSize = int.Parse(strTileSize);
			else
				throw new ArgumentException("TileSize provider setting is required and cannot be empty");

			m_SiteConfig = new SiteConfigFile(m_ConfigFile);
		}

		public string Rewrite(string value)
		{
			NameValueCollection Params = ParseQueryString(value);
			String host = Params.Get("host");

			if(!m_bInit)
			{
				m_bInit = true;
				m_CustomerConfig = new CustomerConfigFile(m_SiteConfig.CustomerConfig, host);
				m_Extent = m_CustomerConfig.Extent;
				m_SiteConfig.ConnectionStrings.TryGetValue(m_CustomerConfig.ConnectionString,out m_ConnectionString);
				Database.SetDBConnection(m_ConnectionString);
			}

			if(((++m_Count) % 5) == 0)
			{
				DataTable dtData = Database.RunQuery("select max(layer_id) as m from mg_layer");
				String sMax = dtData.Rows[0]["m"].ToString();
				return "test/TestRewriteCount.ashx?count=Alastair" + m_Count + ":" + host + ":" + sMax;
			}
			else
				return "test/TestRewriteCount.ashx?count=" + m_Count;
		}

		#endregion

		#region IProviderDescriptor Members

		public IEnumerable<SettingDescriptor> GetSettings()
		{
			yield return new SettingDescriptor("Config", "Location of Site Config File");
			yield return new SettingDescriptor("WMSUrl", "Url of Background WMS");
			yield return new SettingDescriptor("ErrorUrl", "Url of Error Page");
			yield return new SettingDescriptor("TileSize", "Expected Tile Size");
		}

		#endregion

		#region Other Functions

		protected NameValueCollection ParseQueryString(String queryString)
		{
			NameValueCollection urlparams = new NameValueCollection();
			foreach (String pair in queryString.Split('&'))
			{
				String[] key = pair.Split('=');
				if (key.Length == 2)
				{
					urlparams.Add(key[0], key[1]);
				}
			}
			return urlparams;
		}

		protected String FormatQueryString(NameValueCollection wmsParams)
		{
			String strParams = "";
			for (int i = 0; i < wmsParams.Count; i++)
			{
				if (strParams != "") strParams += "&";
				strParams = strParams + (wmsParams.GetKey(i) + "=" + wmsParams.Get(i));
			}
			return strParams;
		}

		protected String ErrorUrl(NameValueCollection wmsParams, String strError)
		{
			wmsParams.Add("error", strError);
			String strQuery = FormatQueryString(wmsParams);
			return String.Format("{0}?{1}", m_ErrorUrl, strQuery);
		}

		#endregion
	}
}
