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
    public class Item : Database.IItem
    {
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

        internal Item(Session Session)
        {
            this._properties = new Dictionary<Model.PropertyType, Property>();
            this.Session = Session;
        }

    }
}
