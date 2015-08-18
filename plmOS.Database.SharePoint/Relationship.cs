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
using System.Xml;
using System.IO;

namespace plmOS.Database.SharePoint
{
    public class Relationship : Item, Database.IRelationship
    {
        public Model.RelationshipType RelationshipType
        {
            get
            {
                return (Model.RelationshipType)this.ItemType;
            }
            internal set
            {
                this.ItemType = value;
            }
        }

        public override Model.ItemType ItemType
        {
            get
            {
                return base.ItemType;
            }
            internal set
            {
                if (value is Model.RelationshipType)
                {
                    base.ItemType = value;
                }
                else
                {
                    throw new ArgumentException("Must be a RelationshipType");
                }
            }
        }

        public Guid ParentBranchID { get; internal set; }

        protected override String FileSuffix
        {
            get
            {
                return "relationship";
            }
        }

        internal override Boolean MatchQuery(Model.Query Query)
        {
            if (((Model.Queries.Relationship)Query).Parent.BranchID.Equals(this.ParentBranchID))
            {
                return base.MatchQuery(Query);
            }
            else
            {
                return false;
            }
        }

        protected override void WriteItemAttributes(System.Xml.XmlDocument doc, System.Xml.XmlElement item)
        {
            base.WriteItemAttributes(doc, item);

            XmlAttribute parentbranchid = doc.CreateAttribute("ParentBranchID");
            parentbranchid.Value = this.ParentBranchID.ToString();
            item.Attributes.Append(parentbranchid);
        }

        protected override void ReadItemAttributes(XmlDocument doc, XmlNode item)
        {
            base.ReadItemAttributes(doc, item);

            this.ParentBranchID = new Guid(item.Attributes["ParentBranchID"].Value);
        } 

        internal Relationship(Session Session, Database.IRelationship Relationship)
            : base(Session, Relationship)
        {
            this.ParentBranchID = Relationship.ParentBranchID;
        }

        internal Relationship(Session Session, FileInfo XMLFile)
            : base(Session, XMLFile)
        {

        }
    }
}
