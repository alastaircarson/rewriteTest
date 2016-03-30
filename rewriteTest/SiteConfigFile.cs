using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace rewriteTest
{
	public class SiteConfigFile
	{
		protected String m_CustomerConfig = "";
		protected Dictionary<String,String> m_ConnectionStrings = new Dictionary<string,string>();

		public SiteConfigFile(String sFilename)
		{
			XmlDocument doc = new XmlDocument();
			doc.Load(sFilename);

			XmlNode node = doc.SelectSingleNode("configuration/appSettings/add[@key=\"siteSettingsPath\"]");

			if(node != null)
				m_CustomerConfig = node.Attributes["value"].Value;
			else
				m_CustomerConfig = "Error Finding Attribute";

			XmlNodeList connections = doc.SelectNodes("configuration/connectionStrings/add");
			foreach(XmlNode n in connections)
			{
				m_ConnectionStrings.Add(n.Attributes["name"].Value,n.Attributes["connectionString"].Value);
			}
		}

		public String CustomerConfig { get { return m_CustomerConfig;} }
		public Dictionary<String, String> ConnectionStrings { get { return m_ConnectionStrings; } }
	}
}
