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
using System.Threading;
using System.Collections.Concurrent;

namespace plmOS.Database.SharePoint
{
    public class Session : Database.ISession
    {
        const int buffersize = 256;

        private object ReadingLock = new object();
        private volatile Boolean _reading;
        public Boolean Reading
        {
            get
            {
                lock (this.ReadingLock)
                {
                    return this._reading;
                }
            }
            private set
            {
                lock (this.ReadingLock)
                {
                    this._reading = value;
                }
            }
        }

        private object WritingLock = new object();
        private volatile Boolean _wrting;
        public Boolean Writing
        {
            get
            {
                lock (this.WritingLock)
                {
                    return this._wrting;
                }
            }
            internal set
            {
                lock (this.WritingLock)
                {
                    this._wrting = value;
                }
            }
        }

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

        public System.Security.SecureString Password { get; private set; }

        private Int32 _syncDelay;
        public Int32 SyncDelay
        {
            get
            {
                return this._syncDelay;
            }
            private set
            {
                if (value < 1)
                {
                    this._syncDelay = 1;
                }
                else
                {
                    this._syncDelay = value;
                }
            }
        }

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
                this.LocalRootFolder = new DirectoryInfo(this._localCache.FullName + "\\" + this.URL.Host + this.URL.AbsolutePath.Replace('/', '\\') + "\\Database");

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

        private ClientContext CreateContext()
        {
            // Create SharePoint Context
            ClientContext SPContext = new ClientContext(this.URL.Scheme + "://" + this.URL.Host);
            SPContext.Credentials = new SharePointOnlineCredentials(this.Username, this.Password);
            return SPContext;
        }

        private Folder OpenBaseFolder(ClientContext Context)
        {
            // Open Base Folder
            Folder SPBaseFolder = Context.Web.GetFolderByServerRelativeUrl(this.URL.AbsolutePath);
            Context.Load(SPBaseFolder);
            Context.ExecuteQuery();
            return SPBaseFolder;
        }

        private Object folderlock = new Object();

        private Folder OpenFolder(ClientContext Context, Folder BaseFolder, String Name)
        {
            lock (this.folderlock)
            {
                Folder SPRootFolder = null;

                try
                {
                    // Ensure SPRootFolder Exists
                    SPRootFolder = BaseFolder.Folders.GetByUrl(Name);
                    Context.Load(SPRootFolder);
                    Context.ExecuteQuery();
                }
                catch (Microsoft.SharePoint.Client.ServerException)
                {
                    // CreateSPRoot Folder
                    SPRootFolder = BaseFolder.Folders.Add(Name);
                    Context.Load(SPRootFolder);
                    Context.ExecuteQuery();
                }

                return SPRootFolder;
            }
        }

        private Thread UploadThread;

        private ConcurrentQueue<Int64> UploadQueue;

        internal void AddToUploadQueue(Transaction Transaction)
        {
            this.UploadQueue.Enqueue(Transaction.ComittedTime);
        }

        private void Upload()
        {
            // Add Existing Cached Transactions to Upload Queue
            foreach (DirectoryInfo transactiondir in this.LocalRootFolder.GetDirectories())
            {
                Int64 transactiondate = -1;

                if (Int64.TryParse(transactiondir.Name, out transactiondate))
                {
                    this.UploadQueue.Enqueue(transactiondate);
                }
            }

            // Open SharePoint Context
            ClientContext SPContext = this.CreateContext();

            // Open Base Folder
            Folder SPBaseFolder = this.OpenBaseFolder(SPContext);

            // Open Root Folder
            Folder SPRootFolder = this.OpenFolder(SPContext, SPBaseFolder, "Database");

            // Open Vault Folder
            Folder SPVaultFolder = this.OpenFolder(SPContext, SPRootFolder, "Vault");

            while (true)
            {
                while (this.UploadQueue.Count > 0)
                {
                    this.Writing = true;
                    Int64 transactiondate = -1;

                    if (this.UploadQueue.TryPeek(out transactiondate))
                    {
                        DirectoryInfo transactiondir = new DirectoryInfo(this.LocalRootFolder.FullName + "\\" + transactiondate.ToString());

                        FileInfo committed = new FileInfo(transactiondir.FullName + "\\committed");

                        if (committed.Exists)
                        {
                            // Open Transaction Folder on SharePoint
                            Folder SPTransactionFolder = this.OpenFolder(SPContext, SPRootFolder, transactiondate.ToString());
                            SPContext.Load(SPTransactionFolder.Files);
                            SPContext.ExecuteQuery();

                            Boolean committedexists = false;

                            foreach (Microsoft.SharePoint.Client.File spfile in SPTransactionFolder.Files)
                            {
                                if (spfile.Name == "committed")
                                {
                                    committedexists = true;
                                    break;
                                }
                            }

                            if (!committedexists)
                            {
                                // Upload XML and Vault files to SharePoint
                                this.Writing = true;

                                foreach (FileInfo xmlfile in transactiondir.GetFiles("*.xml"))
                                {
                                    Boolean spfileexists = false;

                                    foreach (Microsoft.SharePoint.Client.File spfile in SPTransactionFolder.Files)
                                    {
                                        if (spfile.Name == xmlfile.Name)
                                        {
                                            spfileexists = true;
                                            break;
                                        }
                                    }

                                    if (!spfileexists)
                                    {
                                        if (xmlfile.Name.EndsWith(".file.xml"))
                                        {
                                            // Upload Vault File
                                            FileInfo vaultfile = new FileInfo(this.LocalVaultFolder.FullName + "\\" + xmlfile.Name.Replace(".file.xml", ".dat"));

                                            if (vaultfile.Exists)
                                            {
                                                using (FileStream sr = System.IO.File.OpenRead(vaultfile.FullName))
                                                {
                                                    Microsoft.SharePoint.Client.File.SaveBinaryDirect(SPContext, SPVaultFolder.ServerRelativeUrl + "/" + vaultfile.Name, sr, true);
                                                }
                                            }
                                        }

                                        // Upload XML File
                                        using (FileStream sr = System.IO.File.OpenRead(xmlfile.FullName))
                                        {
                                            Microsoft.SharePoint.Client.File.SaveBinaryDirect(SPContext, SPTransactionFolder.ServerRelativeUrl + "/" + xmlfile.Name, sr, true);
                                        }
                                    }
                                }

                                // Upload Commited File
                                using (FileStream sr = System.IO.File.OpenRead(committed.FullName))
                                {
                                    Microsoft.SharePoint.Client.File.SaveBinaryDirect(SPContext, SPTransactionFolder.ServerRelativeUrl + "/" + committed.Name, sr, true);
                                }
                            }

                            // Completed - remove Transaction from Queue
                            this.UploadQueue.TryDequeue(out transactiondate);
                        }
                    }
                }

                this.Writing = false;

                // Sleep
                Thread.Sleep(100);
            }
        }

        private Thread DownloadThread;

        private List<Int64> Downloaded;

        private void Download()
        {
            byte[] buffer = new byte[buffersize];
            int sizeread = 0;

            // Open SharePoint Context
            ClientContext SPContext = this.CreateContext();

            // Open Base Folder
            Folder SPBaseFolder = this.OpenBaseFolder(SPContext);

            // Open Root Folder
            Folder SPRootFolder = this.OpenFolder(SPContext, SPBaseFolder, "Database");

            // Open Vault Folder
            Folder SPVaultFolder = this.OpenFolder(SPContext, SPRootFolder, "Vault");

            while (true)
            {
                // Get Listing of Folders on SharePoint
                SPContext.Load(SPRootFolder.Folders);
                SPContext.ExecuteQuery();

                foreach(Folder transactionfolder in SPRootFolder.Folders)
                {
                    Int64 transactiondate = -1;

                    if (Int64.TryParse(transactionfolder.Name, out transactiondate))
                    {
                        if (!this.Downloaded.Contains(transactiondate))
                        {
                            // Check if Transaction Folder Exists in Local Cache
                            Boolean downloadneeded = true;

                            DirectoryInfo localtransactionfolder = new DirectoryInfo(this.LocalRootFolder.FullName + "\\" + transactiondate.ToString());
                            FileInfo committed = new FileInfo(localtransactionfolder.FullName + "\\committed");

                            if (localtransactionfolder.Exists)
                            {
                                if (committed.Exists)
                                {
                                    downloadneeded = false;
                                }
                            }
                            else
                            {
                                localtransactionfolder.Create();
                            }

                            if (downloadneeded)
                            {
                                // Load File List from SharePoint
                                SPContext.Load(transactionfolder.Files);
                                SPContext.ExecuteQuery();

                                // Check that committed file exists on SharePoint
                                Boolean spcommittedexists = false;

                                foreach (Microsoft.SharePoint.Client.File spfile in transactionfolder.Files)
                                {
                                    if (spfile.Name == "committed")
                                    {
                                        spcommittedexists = true;
                                        break;
                                    }
                                }

                                if (spcommittedexists)
                                {
                                    foreach (Microsoft.SharePoint.Client.File spfile in transactionfolder.Files)
                                    {
                                        if (spfile.Name != "committed")
                                        {
                                            if (spfile.Name.EndsWith(".file.xml"))
                                            {
                                                // Download File from Vault
                                                FileInfo localvault = new FileInfo(this.LocalVaultFolder.FullName + "\\" + spfile.Name.Replace(".file.xml", ".dat"));
                                                Microsoft.SharePoint.Client.FileInformation vaultfileInfo = Microsoft.SharePoint.Client.File.OpenBinaryDirect(SPContext, SPVaultFolder.ServerRelativeUrl + "/" + localvault.Name);

                                                using (FileStream sw = System.IO.File.OpenWrite(localvault.FullName))
                                                {
                                                    using (vaultfileInfo.Stream)
                                                    {
                                                        while((sizeread = vaultfileInfo.Stream.Read(buffer, 0, buffersize)) > 0)
                                                        {
                                                            sw.Write(buffer, 0, sizeread);
                                                        }
                                                    }
                                                }
                                            }

                                            // Download XML File
                                            FileInfo localxml = new FileInfo(localtransactionfolder.FullName + "\\" + spfile.Name);
                                            Microsoft.SharePoint.Client.FileInformation fileInfo = Microsoft.SharePoint.Client.File.OpenBinaryDirect(SPContext, spfile.ServerRelativeUrl);

                                            using (FileStream sw = System.IO.File.OpenWrite(localxml.FullName))
                                            {
                                                using (fileInfo.Stream)
                                                {
                                                    while ((sizeread = fileInfo.Stream.Read(buffer, 0, buffersize)) > 0)
                                                    {
                                                        sw.Write(buffer, 0, sizeread);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // Create Local committed
                                    committed.Create();

                                    this.Downloaded.Add(transactiondate);
                                }
                            }
                            else
                            {
                                this.Downloaded.Add(transactiondate);
                            }
                        }
                    }
                }

                // Set Reading to false once done one Sync
                this.Reading = false;

                Thread.Sleep(this.SyncDelay * 1000);
            }
        }

        public void Dispose()
        {

        }

        public Session(Uri URL, String Username, String Password, DirectoryInfo LocalCache, Int32 SyncDelay)
        {
            this.ItemTypeCache = new Dictionary<string, Model.ItemType>();
            this.ItemCache = new Dictionary<Model.ItemType, Dictionary<Guid, Item>>();
            this.Loaded = new List<Int64>();
            this.UploadQueue = new ConcurrentQueue<Int64>();
            this.Downloaded = new List<Int64>();

            this.URL = URL;
            this.Username = Username;

            // Store Secure Password
            this.Password = new System.Security.SecureString();

            foreach (char c in Password.ToCharArray())
            {
                this.Password.AppendChar(c);
            }

            this.LocalCache = LocalCache;
            this.SyncDelay = SyncDelay;

            this.Reading = true;
            this.Writing = true;

            // Start Upload
            this.UploadThread = new Thread(this.Upload);
            this.UploadThread.IsBackground = true;
            this.UploadThread.Start();

            // Start Download
            this.DownloadThread = new Thread(this.Download);
            this.DownloadThread.IsBackground = true;
            this.DownloadThread.Start();
        }
    }
}
