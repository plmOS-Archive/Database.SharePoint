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
using Microsoft.SharePoint.Client;

namespace plmOS.Database.SharePoint
{
    public class Session : Database.ISession
    {
        public void Create(Model.ItemType ItemType)
        {
            throw new NotImplementedException();
        }

        public void Create(Model.RelationshipType RelationshipType)
        {
            throw new NotImplementedException();
        }

        public void Create(IItem Item, ITransaction Transaction)
        {
            throw new NotImplementedException();
        }

        public void Supercede(IItem Item, ITransaction Transaction)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IItem> Get(Model.Queries.Item Query)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IRelationship> Get(Model.Queries.Relationship Query)
        {
            throw new NotImplementedException();
        }

        public FileStream ReadFromVault(IFile File)
        {
            throw new NotImplementedException();
        }

        public FileStream WriteToVault(IFile File)
        {
            throw new NotImplementedException();
        }

        public ITransaction BeginTransaction()
        {
            throw new NotImplementedException();
        }

        public String URL { get; private set; }

        public String LibraryName { get; private set; }

        public String Username { get; private set; }

        public String Password { get; private set; }

        internal ClientContext Context { get; private set; }

        internal List Library { get; private set; }

        private void Login()
        {
            System.Security.SecureString SecurePassword = new System.Security.SecureString();

            foreach (char c in this.Password.ToCharArray())
            {
                SecurePassword.AppendChar(c);
            }

            // Connect to SharePoint
            this.Context = new ClientContext(this.URL);
            this.Context.Credentials = new SharePointOnlineCredentials(this.Username, SecurePassword);
            
            this.Library = this.Context.Web.Lists.GetByTitle(this.LibraryName);
            this.Context.Load(this.Library);
            this.Context.ExecuteQuery();
        }

        public Session(String URL, String LibraryName, String Username, String Password)
        {
            this.URL = URL;
            this.LibraryName = LibraryName;
            this.Username = Username;
            this.Password = Password;
            this.Login();
        }
    }
}
