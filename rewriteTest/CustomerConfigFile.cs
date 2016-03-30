using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace rewriteTest
{
	public class CustomerConfigFile
	{
		protected String m_Extent = "";
		protected String m_ConnectionString = "";

		public CustomerConfigFile(String sFilename, String sHost)
		{
			XmlDocument doc = new XmlDocument();
			doc.Load(sFilename);

			XmlNode customerNode = doc.SelectSingleNode("customers/customer[@id=\"" + sHost + "\"]");

			if (customerNode != null)
			{
				XmlNode extentNode = customerNode.SelectSingleNode("./limitEnvelope");
				if(extentNode != null)
				{
					m_Extent = extentNode.InnerText;
				}
				else
				{
					m_Extent = "No Limit Extent";
				}

				XmlNode connectionNode = customerNode.SelectSingleNode("./connectionStringName");
				if(connectionNode != null)
				{
					m_ConnectionString = connectionNode.InnerText;
				}
				else
				{
					m_ConnectionString = "No Connection String";
				}
			}
			else
			{
				m_Extent = "No Matching Customer";
				m_ConnectionString = "No Matching Customer";
			}
		}

		public String Extent { get { return m_Extent; } }
		public String ConnectionString { get { return m_ConnectionString; } }
	}
}
