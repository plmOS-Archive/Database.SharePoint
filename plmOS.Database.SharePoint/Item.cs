/*  
  plmOS Database SharePoint is a .NET library that implements a Microsoft SharePoint plmOS Database.

  Copyright (C) 2015 Processwall Limited.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published
  by the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see http://opensource.org/licenses/AGPL-3.0.
 
  Company: Processwall Limited
  Address: The Winnowing House, Mill Lane, Askham Richard, York, YO23 3NW, United Kingdom
  Tel:     +44 113 815 3440
  Email:   support@processwall.com
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Xml;

namespace plmOS.Database.SharePoint
{
    public class Item : Database.IItem
    {
        internal static String DateFormat = "yyyy-MM-ddTHH:mm:ss.fff";

        internal Session Session { get; private set; }

        public virtual Model.ItemType ItemType { get; internal set; }

        public Guid ItemID { get; internal set; }

        public Guid BranchID { get; internal set; }

        public Guid VersionID { get; internal set; }

        public Int64 Branched { get; internal set; }

        public Int64 Versioned { get; internal set; }

        public Int64 Superceded { get; internal set; }

        private Dictionary<Model.PropertyType, Property> _properties;

        public IEnumerable<IProperty> Properties
        {
            get
            {
                return this._properties.Values;
            }
        }

        public IProperty Property(Model.PropertyType PropertyType)
        {
            return this._properties[PropertyType];
        }

        internal void AddProperty(Property Property)
        {
            this._properties[Property.PropertyType] = Property;
        }

        private Boolean MatchCondition(Model.Condition Condition)
        {
            switch(Condition.GetType().Name)
            {
                case "Property":

                    Model.PropertyType proptype = this.ItemType.PropertyType(((Model.Conditions.Property)Condition).Name);
                    Property property = (Property)this.Property(proptype);

                    switch(property.PropertyType.Type)
                    {
                        case Model.PropertyTypeValues.String:
                            String propvalue = (String)property.Object;
                            String conditionvalue = (String)(((Model.Conditions.Property)Condition).Value);

                            switch (((Model.Conditions.Property)Condition).Operator)
                            {
                                case Model.Conditions.Operators.eq:
                                    return (String.Compare(propvalue, conditionvalue, true) == 0);
                                case Model.Conditions.Operators.ge:
                                    return ((String.Compare(propvalue, conditionvalue, true) == 1) || (String.Compare(propvalue, conditionvalue, true) == 0));
                                case Model.Conditions.Operators.gt:
                                    return (String.Compare(propvalue, conditionvalue, true) == 1);
                                case Model.Conditions.Operators.le:
                                    return ((String.Compare(propvalue, conditionvalue, true) == -1) || (String.Compare(propvalue, conditionvalue, true) == 0));
                                case Model.Conditions.Operators.lt:
                                    return (String.Compare(propvalue, conditionvalue, true) == -1);
                                case Model.Conditions.Operators.ne:
                                    return (String.Compare(propvalue, conditionvalue, true) != 0);
                                default:
                                    throw new NotImplementedException("Condition Operator not implemeted: " + ((Model.Conditions.Property)Condition).Operator);
                            }

                        default:
                            throw new NotImplementedException("PropertyType not implemented: " + property.PropertyType.Type);
                    }

                default:
                    throw new NotImplementedException("Condition Type not implemneted: " + Condition.GetType().Name);
            }
        }

        internal virtual Boolean MatchQuery(Model.Query Query)
        {
            if (this.Superceded == -1)
            {
                if (Query.Condition != null)
                {
                    return this.MatchCondition(Query.Condition);
                }
                else
                {
                    return true;
                }
            }
            else
            {
                return false;
            }
        }

        protected virtual void WriteItemAttributes(XmlDocument doc, XmlElement item)
        {
            XmlAttribute itemtype = doc.CreateAttribute("ItemType");
            itemtype.Value = this.ItemType.Name;
            item.Attributes.Append(itemtype);

            XmlAttribute itemid = doc.CreateAttribute("ItemID");
            itemid.Value = this.ItemID.ToString();
            item.Attributes.Append(itemid);

            XmlAttribute branchid = doc.CreateAttribute("BranchID");
            branchid.Value = this.BranchID.ToString();
            item.Attributes.Append(branchid);

            XmlAttribute versionid = doc.CreateAttribute("VersionID");
            versionid.Value = this.VersionID.ToString();
            item.Attributes.Append(versionid);

            XmlAttribute branched = doc.CreateAttribute("Branched");
            branched.Value = this.Branched.ToString();
            item.Attributes.Append(branched);

            XmlAttribute versioned = doc.CreateAttribute("Versioned");
            versioned.Value = this.Versioned.ToString();
            item.Attributes.Append(versioned);

            XmlAttribute superceded = doc.CreateAttribute("Superceded");
            superceded.Value = this.Superceded.ToString();
            item.Attributes.Append(superceded);
        }

        protected virtual String FileSuffix
        {
            get
            {
                return "item";
            }
        }

        internal void Write(DirectoryInfo Directory)
        {
            XmlDocument doc = new XmlDocument();
            XmlNode docNode = doc.CreateXmlDeclaration("1.0", "UTF-8", null);
            doc.AppendChild(docNode);

            XmlElement item = doc.CreateElement("Item");
            doc.AppendChild(item);

            this.WriteItemAttributes(doc, item);

            XmlElement properties = doc.CreateElement("Properties");
            item.AppendChild(properties);

            foreach(Property prop in this.Properties)
            {
                XmlElement property = doc.CreateElement("Property");
                properties.AppendChild(property);

                XmlAttribute name = doc.CreateAttribute("Name");
                name.Value = prop.PropertyType.Name;
                property.Attributes.Append(name);

                XmlAttribute value = doc.CreateAttribute("Value");
                property.Attributes.Append(value);

                if (prop.Object != null)
                {
                    switch (prop.PropertyType.Type)
                    {
                        case Model.PropertyTypeValues.DateTime:
                            value.Value = ((DateTime)prop.Object).ToString(DateFormat);
                            break;
                        case Model.PropertyTypeValues.Double:
                        case Model.PropertyTypeValues.String:
                        case Model.PropertyTypeValues.Item:
                        case Model.PropertyTypeValues.List:
                        case Model.PropertyTypeValues.Boolean:
                            value.Value = prop.Object.ToString();
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }
            }

            doc.Save(Directory.FullName + "\\" + this.VersionID + "." + this.FileSuffix + ".xml");
        }

        protected virtual void ReadItemAttributes(XmlDocument doc, XmlNode item)
        {
            this.ItemType = this.Session.ItemType(item.Attributes["ItemType"].Value);
            this.ItemID = Guid.Parse(item.Attributes["ItemID"].Value);
            this.BranchID = Guid.Parse(item.Attributes["BranchID"].Value);
            this.VersionID = Guid.Parse(item.Attributes["VersionID"].Value);
            this.Branched = Int64.Parse(item.Attributes["Branched"].Value);
            this.Versioned = Int64.Parse(item.Attributes["Versioned"].Value);
            this.Superceded = Int64.Parse(item.Attributes["Superceded"].Value);
        }

        private void Read(FileInfo XMLFile)
        {
            // Load XML
            XmlDocument doc = new XmlDocument();
            doc.Load(XMLFile.FullName);
            
            // Get Item Node
            XmlNode item = doc.SelectSingleNode("Item");

            // Load Item Attributes
            this.ReadItemAttributes(doc, item);

            // Load Properties
            this._properties = new Dictionary<Model.PropertyType, Property>();

            XmlNode properties = item.SelectSingleNode("Properties");

            foreach(XmlNode property in properties.ChildNodes)
            {
                Model.PropertyType proptype = this.ItemType.PropertyType(property.Attributes["Name"].Value);
                Object value = null;

                if (property.Attributes["Value"].Value != null)
                {
                    switch (proptype.Type)
                    {
                        case Model.PropertyTypeValues.DateTime:

                            if (!String.IsNullOrEmpty(property.Attributes["Value"].Value))
                            {
                                value = DateTime.Parse(property.Attributes["Value"].Value);
                            }

                            break;

                        case Model.PropertyTypeValues.Double:
                            value = Double.Parse(property.Attributes["Value"].Value);
                            break;

                        case Model.PropertyTypeValues.Item:
                            value = Guid.Parse(property.Attributes["Value"].Value);
                            break;

                        case Model.PropertyTypeValues.String:
                            value = property.Attributes["Value"].Value;
                            break;

                        case Model.PropertyTypeValues.List:
                            value = Int32.Parse(property.Attributes["Value"].Value);
                            break;

                        case Model.PropertyTypeValues.Boolean:
                            value = Boolean.Parse(property.Attributes["Value"].Value);
                            break;

                        default:
                            throw new NotImplementedException("Propertype not implemented: " + proptype.Type.ToString());
                    }
                }
                
                this._properties[proptype] = new Property(this, proptype, value);
            }
        }

        internal Item(Session Session, Database.IItem Item)
        {
            this._properties = new Dictionary<Model.PropertyType, Property>();
            this.Session = Session;
            this.ItemType = Item.ItemType;
            this.ItemID = Item.ItemID;
            this.BranchID = Item.BranchID;
            this.VersionID = Item.VersionID;
            this.Branched = Item.Branched;
            this.Versioned = Item.Versioned;
            this.Superceded = Item.Superceded;

            foreach(Database.IProperty prop in Item.Properties)
            {
                this._properties[prop.PropertyType] = new Property(this, prop.PropertyType, prop.Object);
            }
        }

        internal Item(Session Session, FileInfo XMLFile)
        {
            this.Session = Session;
            this.Read(XMLFile);
        }
    }
}
