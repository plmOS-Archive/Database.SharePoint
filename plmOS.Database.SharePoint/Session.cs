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
        private Dictionary<String, Model.ItemType> ItemTypeCache;

        internal Model.ItemType ItemType(String Name)
        {
            return this.ItemTypeCache[Name];
        }

        private Dictionary<Model.ItemType, Dictionary<Guid, Item>> ItemCache;

        private void AddItemToCache(Item Item)
        {
            if (!this.ItemCache.ContainsKey(Item.ItemType))
            {
                this.ItemCache[Item.ItemType] = new Dictionary<Guid, Item>();
            }

            this.ItemCache[Item.ItemType][Item.VersionID] = Item;
        }

        public void Create(Model.ItemType ItemType)
        {
            this.ItemTypeCache[ItemType.Name] = ItemType;
        }

        public void Create(Model.RelationshipType RelationshipType)
        {
            this.ItemTypeCache[RelationshipType.Name] = RelationshipType;
        }

        public void Create(IItem Item, ITransaction Transaction)
        {
            Item thisitem = new Item(this, Item);
            this.AddItemToCache(thisitem);
            ((Transaction)Transaction).AddItem(thisitem);
        }

        public void Create(IRelationship Relationship, ITransaction Transaction)
        {
            Relationship thisrel = new Relationship(this, Relationship);
            this.AddItemToCache(thisrel);
            ((Transaction)Transaction).AddItem(thisrel);
        }

        public void Create(IFile File, ITransaction Transaction)
        {
            File thisfile = new File(this, File);
            this.AddItemToCache(thisfile);
            ((Transaction)Transaction).AddItem(thisfile);
        }

        public void Supercede(IItem Item, ITransaction Transaction)
        {
            Item databaseitem = this.ItemCache[Item.ItemType][Item.VersionID];
            databaseitem.Superceded = Item.Superceded;
            ((Transaction)Transaction).AddItem(databaseitem);
        }

        public IItem Get(Model.ItemType ItemType, Guid BranchID)
        {
            this.Load();

            if (this.ItemCache.ContainsKey(ItemType))
            {
                foreach (Item item in this.ItemCache[ItemType].Values)
                {
                    if (item.Superceded == -1 && item.BranchID == BranchID)
                    {
                        return item;
                    }
                }
            }

            return null;
        }

        public IEnumerable<IItem> Get(Model.Queries.Item Query)
        {
            this.Load();

            List<Item> ret = new List<Item>();

            if (this.ItemCache.ContainsKey(Query.ItemType))
            {
                foreach(Item item in this.ItemCache[Query.ItemType].Values)
                {
                    if (item.MatchQuery(Query))
                    {
                        ret.Add(item);
                    }
                }
            }

            return ret;
        }

        public IEnumerable<IRelationship> Get(Model.Queries.Relationship Query)
        {
            this.Load();

            List<Relationship> ret = new List<Relationship>();

            if (this.ItemCache.ContainsKey(Query.ItemType))
            {
                foreach (Item item in this.ItemCache[Query.ItemType].Values)
                {
                    if (((Relationship)item).MatchQuery(Query))
                    {
                        ret.Add((Relationship)item);
                    }
                }
            }

            return ret;
        }

        public FileStream ReadFromVault(IFile File)
        {
            return new FileStream(this.LocalVaultFolder.FullName + "\\" + File.VersionID.ToString() + ".dat", FileMode.Open);
        }

        public FileStream WriteToVault(IFile File)
        {
            return new FileStream(this.LocalVaultFolder.FullName + "\\" + File.VersionID.ToString() + ".dat", FileMode.Create);
        }

        public ITransaction BeginTransaction()
        {
            return new Transaction(this);
        }

        public Uri URL { get; private set; }

        public String Username { get; private set; }

        public String Password { get; private set; }

        private DirectoryInfo _localCache;
        public DirectoryInfo LocalCache 
        { 
            get
            {
                return this._localCache;
            }
            private set
            {
                this._localCache = value;

                // Ensure Local Cache Exists
                if (!this._localCache.Exists)
                {
                    this._localCache.Create();
                }

                // Set Local Root Folder and ensure exists
                this.LocalRootFolder = new DirectoryInfo(this._localCache.FullName + "\\" + this.URL.Host + this.URL.AbsolutePath + "\\Database");

                if (!this.LocalRootFolder.Exists)
                {
                    this.LocalRootFolder.Create();
                }

                // Set LocalVaultFolder and ensure exists
                this.LocalVaultFolder = new DirectoryInfo(this.LocalRootFolder.FullName + "\\Vault");

                if (!this.LocalVaultFolder.Exists)
                {
                    this.LocalVaultFolder.Create();
                }
            }
        }

        internal DirectoryInfo LocalRootFolder { get; private set; }

        internal DirectoryInfo LocalVaultFolder { get; private set; }

        internal ClientContext SPContext { get; private set; }

        internal Folder SPRootFolder { get; private set; }

        private void Login()
        {
            // Create Secure Password
            System.Security.SecureString SecurePassword = new System.Security.SecureString();

            foreach (char c in this.Password.ToCharArray())
            {
                SecurePassword.AppendChar(c);
            }

            // Connect to SharePoint
            this.SPContext = new ClientContext(this.URL.Scheme + "://" + this.URL.Host);
            this.SPContext.Credentials = new SharePointOnlineCredentials(this.Username, SecurePassword);

            // Open Base Folder
            Folder basefolder = this.SPContext.Web.GetFolderByServerRelativeUrl(this.URL.AbsolutePath);
            this.SPContext.Load(basefolder);
            this.SPContext.ExecuteQuery(); 

            try
            {
                // Ensure SPRootFolder Exists
                this.SPRootFolder = basefolder.Folders.GetByUrl("Database");
                this.SPContext.Load(this.SPRootFolder);
                this.SPContext.ExecuteQuery();
            }
            catch (Microsoft.SharePoint.Client.ServerException)
            {
                // CreateSPRoot Folder
                this.SPRootFolder = basefolder.Folders.Add("Database");
                this.SPContext.Load(this.SPRootFolder);
                this.SPContext.ExecuteQuery();
            }
        }

        private List<Int64> Loaded;
        private void Load()
        {
            foreach (DirectoryInfo transactiondir in this.LocalRootFolder.GetDirectories())
            {
                Int64 transactiondate = -1;

                if (Int64.TryParse(transactiondir.Name, out transactiondate))
                {
                    if (!Loaded.Contains(transactiondate))
                    {
                        FileInfo committed = new FileInfo(transactiondir.FullName + "\\committed");

                        if (committed.Exists)
                        {
                            foreach (FileInfo xmlfile in transactiondir.GetFiles("*.item.xml"))
                            {
                                Item item = new Item(this, xmlfile);
                                this.AddItemToCache(item);
                            }

                            foreach (FileInfo xmlfile in transactiondir.GetFiles("*.file.xml"))
                            {
                                File item = new File(this, xmlfile);
                                this.AddItemToCache(item);
                            }

                            foreach (FileInfo xmlfile in transactiondir.GetFiles("*.relationship.xml"))
                            {
                                Relationship item = new Relationship(this, xmlfile);
                                this.AddItemToCache(item);
                            }

                            this.Loaded.Add(transactiondate);
                        }
                    }
                }
            }
        }

        public Session(Uri URL, String Username, String Password, DirectoryInfo LocalCache)
        {
            this.ItemTypeCache = new Dictionary<string, Model.ItemType>();
            this.ItemCache = new Dictionary<Model.ItemType, Dictionary<Guid, Item>>();
            this.Loaded = new List<Int64>();

            this.URL = URL;
            this.Username = Username;
            this.Password = Password;
            this.LocalCache = LocalCache;
        }
    }
}
