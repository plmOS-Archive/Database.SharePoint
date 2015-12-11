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
using System.ComponentModel;

namespace plmOS.Database.SharePoint
{
    public class Session : Database.ISession
    {
        const int buffersize = 256;

        public event PropertyChangedEventHandler PropertyChanged;

        private void OnPropertyChanged(String Name)
        {
            if (this.PropertyChanged != null)
            {
                this.PropertyChanged.Invoke(this, new PropertyChangedEventArgs(Name));
            }
        }

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
                    if (this._reading != value)
                    {
                        this._reading = value;
                        this.OnPropertyChanged("Reading");
                    }
                }
            }
        }

        private volatile Int32 _readingTotal;
        public Int32 ReadingTotal
        {
            get
            {
                lock(this.ReadingLock)
                {
                    return this._readingTotal;
                }
            }
            private set
            {
                lock(this.ReadingLock)
                {
                    if (this._readingTotal != value)
                    {
                        this._readingTotal = value;
                        this.OnPropertyChanged("ReadingTotal");
                    }
                }
            }
        }

        private volatile Int32 _readingNumber;
        public Int32 ReadingNumber
        {
            get
            {
                lock (this.ReadingLock)
                {
                    return this._readingNumber;
                }
            }
            private set
            {
                lock (this.ReadingLock)
                {
                    if (this._readingNumber != value)
                    {
                        this._readingNumber = value;
                        this.OnPropertyChanged("ReadingNumber");
                    }
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
                    if (this._wrting != value)
                    {
                        this._wrting = value;
                        this.OnPropertyChanged("Writing");
                    }
                }
            }
        }

        private volatile Int32 _writingTotal;
        public Int32 WritingTotal
        {
            get
            {
                lock (this.WritingLock)
                {
                    return this._writingTotal;
                }
            }
            private set
            {
                lock (this.WritingLock)
                {
                    if (this._writingTotal != value)
                    {
                        this._writingTotal = value;
                        this.OnPropertyChanged("WritingTotal");
                    }
                }
            }
        }

        private volatile Int32 _writingNumber;
        public Int32 WritingNumber
        {
            get
            {
                lock (this.WritingLock)
                {
                    return this._writingNumber;
                }
            }
            private set
            {
                lock (this.WritingLock)
                {
                    if (this._writingNumber != value)
                    {
                        this._writingNumber = value;
                        this.OnPropertyChanged("WritingNumber");
                    }
                }
            }
        }


        private Boolean _initialised;
        public Boolean Initialised
        {
            get
            {
                return this._initialised;
            }
            private set
            {

                if (this._initialised != value)
                {
                    this._initialised = value;
                    this.OnPropertyChanged("Initialised");
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
            if (this.Initialised)
            {
                return new Transaction(this);
            }
            else
            {
                throw new Database.NotInitialisedException();
            }
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

        public Logging.Log Log { get; private set; }

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
                this.LocalRootFolder = new DirectoryInfo(this._localCache.FullName + this.URL.AbsolutePath.Replace('/', '\\') + "\\Database");

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
            if (this.Initialised)
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
            else
            {
                throw new Database.NotInitialisedException();
            }
        }

        private static Uri BaseURL(Uri URL)
        {
            String full = URL.AbsoluteUri;
            full = full.TrimEnd(new char[] { '/' });
            int pos = full.LastIndexOf('/');
            return new Uri(full.Substring(0, pos));
        }

        private static String FolderName(Uri URL)
        {
            String full = URL.AbsoluteUri;
            full = full.TrimEnd(new char[] { '/' });
            int pos = full.LastIndexOf('/');
            return full.Substring(pos + 1, full.Length - pos - 1);
        }

        private String _projectID;
        public String ProjectID
        {
            get
            {
                if (this._projectID == null)
                {
                    this._projectID = FolderName(this.URL);
                }

                return this._projectID;
            }
        }

        private String _supplierID;
        public String SupplierID
        {
            get
            {
                if (this._supplierID == null)
                {
                    this._supplierID = FolderName(this.SupplierURL);
                }

                return this._supplierID;
            }
        }

        private Uri _supplierURL;
        public Uri SupplierURL
        {
            get
            {
                if (this._supplierURL == null)
                {
                    this._supplierURL = BaseURL(this.URL);
                }

                return this._supplierURL;
            }
        }

        private Uri _siteURL;
        public Uri SiteURL
        {
            get
            {
                if (this._siteURL == null)
                {
                    this._siteURL = BaseURL(this.SupplierURL);
                }

                return this._siteURL;
            }
        }

        private ClientContext CreateContext()
        {
            // Create SharePoint Context
            ClientContext SPContext = new ClientContext(this.SiteURL.AbsoluteUri);
            SPContext.Credentials = new SharePointOnlineCredentials(this.Username, this.Password);
            return SPContext;
        }

        private Folder OpenBaseFolder(ClientContext Context)
        {
            // Open Base Folder
            Folder SPBaseFolder = Context.Web.RootFolder;
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

            if (this.Writing)
            {
                this.WritingTotal++;
            }
        }

        private void Upload()
        {
            ClientContext SPContext = null;
            Folder SPBaseFolder = null;
            Folder SPSupplierFolder = null;
            Folder SPProjectFolder = null;
            Folder SPRootFolder = null;
            Folder SPVaultFolder = null;

            while (true)
            {
                try
                {
                    this.WritingTotal = this.UploadQueue.Count;
                    this.WritingNumber = 0;

                    while (this.UploadQueue.Count > 0)
                    {
                        this.Writing = true;
                        this.WritingNumber++;

                        if (SPVaultFolder == null)
                        {
                            this.Log.Add(plmOS.Logging.Log.Levels.DEB, "Starting to upload to SharePoint: " + this.URL);

                            // Open SharePoint Context
                            SPContext = this.CreateContext();

                            // Open Base Folder
                            SPBaseFolder = this.OpenBaseFolder(SPContext);

                            // Open Supplier Folder
                            SPSupplierFolder = this.OpenFolder(SPContext, SPBaseFolder, this.SupplierID);

                            // Open Project Folder
                            SPProjectFolder = this.OpenFolder(SPContext, SPSupplierFolder, this.ProjectID);

                            // Open Root Folder
                            SPRootFolder = this.OpenFolder(SPContext, SPProjectFolder, "Database");

                            // Open Vault Folder
                            SPVaultFolder = this.OpenFolder(SPContext, SPRootFolder, "Vault");
                        }

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

                        this.Writing = false;
                    }     
                }
                catch(Exception e)
                {
                    this.Log.Add(plmOS.Logging.Log.Levels.ERR, "SharePoint upload failed: " + e.Message);
                    this.Writing = false;
                }

                // Sleep
                Thread.Sleep(500);
            }
        }

        private Thread DownloadThread;

        private List<Int64> Downloaded;

        private void Download()
        {
            byte[] buffer = new byte[buffersize];
            int sizeread = 0;

            ClientContext SPContext = null;
            Folder SPBaseFolder = null;
            Folder SPSupplierFolder = null;
            Folder SPProjectFolder = null;
            Folder SPRootFolder = null;
            Folder SPVaultFolder = null;

            while (true)
            {
                try
                {
                    if (SPVaultFolder == null)
                    {
                        this.Log.Add(plmOS.Logging.Log.Levels.DEB, "Starting to download from SharePoint: " + this.URL);

                        // Open SharePoint Context
                        SPContext = this.CreateContext();

                        // Open Base Folder
                        SPBaseFolder = this.OpenBaseFolder(SPContext);

                        // Open Supplier Folder
                        SPSupplierFolder = this.OpenFolder(SPContext, SPBaseFolder, this.SupplierID);

                        // Open Project Folder
                        SPProjectFolder = this.OpenFolder(SPContext, SPSupplierFolder, this.ProjectID);

                        // Open Root Folder
                        SPRootFolder = this.OpenFolder(SPContext, SPProjectFolder, "Database");

                        // Open Vault Folder
                        SPVaultFolder = this.OpenFolder(SPContext, SPRootFolder, "Vault");
                    }

                    // Get Listing of Folders on SharePoint
                    SPContext.Load(SPRootFolder.Folders);
                    SPContext.ExecuteQuery();

                    // Create List of Transactions that need to be Downloaded
                    List<Folder> tobedownlaoded = new List<Folder>();

                    foreach (Folder transactionfolder in SPRootFolder.Folders)
                    {
                        Int64 transactiondate = -1;

                        if (Int64.TryParse(transactionfolder.Name, out transactiondate))
                        {
                            if (!this.Downloaded.Contains(transactiondate))
                            {
                                tobedownlaoded.Add(transactionfolder);
                            }
                        }
                    }

                    if (tobedownlaoded.Count > 0)
                    {
                        this.ReadingTotal = tobedownlaoded.Count;
                        this.ReadingNumber = 0;
                        this.Reading = true;

                        foreach (Folder transactionfolder in tobedownlaoded)
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
                                                                while ((sizeread = vaultfileInfo.Stream.Read(buffer, 0, buffersize)) > 0)
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

                            this.ReadingNumber++;
                        }

                        this.Reading = false;
                        this.ReadingTotal = 0;
                        this.ReadingNumber = 0;
                    }

                    // Set Initialised to true once done one Sync
                    if (!this.Initialised)
                    {
                        // Remove any directories from Cache that are not on SharePoint
                        foreach (DirectoryInfo transactiodirectory in this.LocalRootFolder.GetDirectories())
                        {
                            Int64 transactiodate = -1;

                            if (Int64.TryParse(transactiodirectory.Name, out transactiodate))
                            {
                                if (!this.Downloaded.Contains(transactiodate))
                                {
                                    transactiodirectory.Delete(true);
                                }
                            }
                        }

                        // Set to Initialised
                        this.Initialised = true;

                        // Start Upload
                        this.UploadThread = new Thread(this.Upload);
                        this.UploadThread.IsBackground = true;
                        this.UploadThread.Start();
                    }
                }
                catch (Exception e)
                {
                    this.Log.Add(plmOS.Logging.Log.Levels.ERR, "SharePoint download failed: " + e.Message);
                }

                // Delay to next check
                Thread.Sleep(this.SyncDelay * 1000);
            }
        }

        public void Dispose()
        {

        }

        public Session(Uri URL, String Username, String Password, DirectoryInfo LocalCache, Int32 SyncDelay, Logging.Log Log)
        {
            this.Reading = false;
            this.ReadingNumber = 0;
            this.ReadingTotal = 0;
            this.Writing = false;
            this.WritingNumber = 0;
            this.WritingTotal = 0;
            this.Initialised = false;

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
            this.Log = Log;

            this.Log.Add(plmOS.Logging.Log.Levels.DEB, "Opening SharePoint Database: " + this.URL);

            // Start Download
            this.DownloadThread = new Thread(this.Download);
            this.DownloadThread.IsBackground = true;
            this.DownloadThread.Start();
        }
    }
}
