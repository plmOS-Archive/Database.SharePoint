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

        internal Relationship(Session Session)
            : base(Session)
        {

        }
    }
}
