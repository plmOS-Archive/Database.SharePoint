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
using System.Threading;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.IO.Compression;

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

        private object ReadingTotalLock = new object();
        private volatile Int32 _readingTotal;
        public Int32 ReadingTotal
        {
            get
            {
                lock (this.ReadingTotalLock)
                {
                    return this._readingTotal;
                }
            }
            private set
            {
                lock (this.ReadingTotalLock)
                {
                    if (this._readingTotal != value)
                    {
                        this._readingTotal = value;
                        this.OnPropertyChanged("ReadingTotal");
                    }
                }
            }
        }

        private object ReadingNumberLock = new object();
        private volatile Int32 _readingNumber;
        public Int32 ReadingNumber
        {
            get
            {
                lock (this.ReadingNumberLock)
                {
                    return this._readingNumber;
                }
            }
            private set
            {
                lock (this.ReadingNumberLock)
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
        private volatile Boolean _writing;
        public Boolean Writing
        {
            get
            {
                lock (this.WritingLock)
                {
                    return this._writing;
                }
            }
            internal set
            {
                lock (this.WritingLock)
                {
                    if (this._writing != value)
                    {
                        this._writing = value;
                    }
                }

                this.OnPropertyChanged("Writing");
            }
        }

        private object WritingTotalLock = new object();
        private volatile Int32 _writingTotal;
        public Int32 WritingTotal
        {
            get
            {
                lock (this.WritingTotalLock)
                {
                    return this._writingTotal;
                }
            }
            private set
            {
                lock (this.WritingTotalLock)
                {
                    if (this._writingTotal != value)
                    {
                        this._writingTotal = value;
                        this.OnPropertyChanged("WritingTotal");
                    }
                }
            }
        }

        private object WritingNumberLock = new object();
        private volatile Int32 _writingNumber;
        public Int32 WritingNumber
        {
            get
            {
                lock (this.WritingNumberLock)
                {
                    return this._writingNumber;
                }
            }
            private set
            {
                lock (this.WritingNumberLock)
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

        private Microsoft.SharePoint.Client.Folder OpenBaseFolder(Microsoft.SharePoint.Client.ClientContext Context)
        {
            // Open Base Folder
            Microsoft.SharePoint.Client.Folder SPBaseFolder = Context.Web.RootFolder;
            Context.Load(SPBaseFolder);
            Context.ExecuteQuery();
            return SPBaseFolder;
        }

        private Microsoft.SharePoint.Client.Folder OpenFolder(Microsoft.SharePoint.Client.ClientContext Context, Microsoft.SharePoint.Client.Folder BaseFolder, String Name)
        {
            Microsoft.SharePoint.Client.Folder SPRootFolder = null;

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

                        this.Log.Add(plmOS.Logging.Log.Levels.DEB, "Starting to upload to SharePoint: " + this.URL);

                        Int64 transactiondate = -1;

                        if (this.UploadQueue.TryPeek(out transactiondate))
                        {
                            DirectoryInfo transactiondir = new DirectoryInfo(this.LocalRootFolder.FullName + "\\" + transactiondate.ToString());

                            FileInfo committed = new FileInfo(transactiondir.FullName + "\\committed");

                            if (committed.Exists)
                            {
                                using (Microsoft.SharePoint.Client.ClientContext SPContext = new Microsoft.SharePoint.Client.ClientContext(this.SiteURL.AbsoluteUri))
                                {
                                    SPContext.Credentials = new Microsoft.SharePoint.Client.SharePointOnlineCredentials(this.Username, this.Password);

                                    // Open Base Folder
                                    Microsoft.SharePoint.Client.Folder SPBaseFolder = this.OpenBaseFolder(SPContext);

                                    // Open Supplier Folder
                                    Microsoft.SharePoint.Client.Folder SPSupplierFolder = this.OpenFolder(SPContext, SPBaseFolder, this.SupplierID);

                                    // Open Project Folder
                                    Microsoft.SharePoint.Client.Folder SPProjectFolder = this.OpenFolder(SPContext, SPSupplierFolder, this.ProjectID);

                                    // Open Root Folder
                                    Microsoft.SharePoint.Client.Folder SPRootFolder = this.OpenFolder(SPContext, SPProjectFolder, "Database");

                                    // Check for Transaction File on SharePoint
                                    SPContext.Load(SPRootFolder.Files);
                                    SPContext.ExecuteQuery();

                                    Boolean committedexists = false;

                                    foreach (Microsoft.SharePoint.Client.File spfile in SPRootFolder.Files)
                                    {
                                        if (spfile.Name == transactiondate.ToString() + ".committed")
                                        {
                                            committedexists = true;
                                            break;
                                        }
                                    }

                                    if (!committedexists)
                                    {
                                        // Create Temp Folder
                                        DirectoryInfo tmptransactiondir = new DirectoryInfo(transactiondir.FullName + ".upload");

                                        if (tmptransactiondir.Exists)
                                        {
                                            foreach (FileInfo file in tmptransactiondir.GetFiles())
                                            {
                                                file.Delete();
                                            }
                                        }
                                        else
                                        {
                                            tmptransactiondir.Create();
                                        }

                                        // Copy XML Files and Vault Files to temp folder
                                        foreach (FileInfo xmlfile in transactiondir.GetFiles("*.xml"))
                                        {
                                            if (xmlfile.Name.EndsWith(".file.xml"))
                                            {
                                                // Copy Vault File
                                                FileInfo vaultfile = new FileInfo(this.LocalVaultFolder.FullName + "\\" + xmlfile.Name.Replace(".file.xml", ".dat"));
                                                vaultfile.CopyTo(tmptransactiondir.FullName + "\\" + vaultfile.Name);
                                            }

                                            xmlfile.CopyTo(tmptransactiondir.FullName + "\\" + xmlfile.Name);
                                        }

                                        // Create ZIP File
                                        FileInfo transactionzipfile = new FileInfo(transactiondir.FullName + ".zip");

                                        if (transactionzipfile.Exists)
                                        {
                                            transactionzipfile.Delete();
                                        }

                                        ZipFile.CreateFromDirectory(tmptransactiondir.FullName, transactionzipfile.FullName);

                                        // Upload ZIP File
                                        using (FileStream sr = System.IO.File.OpenRead(transactionzipfile.FullName))
                                        {
                                            Microsoft.SharePoint.Client.File.SaveBinaryDirect(SPContext, SPRootFolder.ServerRelativeUrl + "/" + transactionzipfile.Name, sr, true);
                                        }

                                        // Upload Comitted File
                                        using (FileStream sr = System.IO.File.OpenRead(committed.FullName))
                                        {
                                            Microsoft.SharePoint.Client.File.SaveBinaryDirect(SPContext, SPRootFolder.ServerRelativeUrl + "/" + Path.GetFileNameWithoutExtension(transactionzipfile.Name) + ".comitted", sr, true);
                                        }

                                        // Delete ZIP File
                                        transactionzipfile.Delete();

                                        // Delete Temp Folder
                                        tmptransactiondir.Delete(true);
                                    }

                                    // Completed - remove Transaction from Queue
                                    this.UploadQueue.TryDequeue(out transactiondate);
                                }
                            }
                        }

                        this.Writing = false;
                    }
                }
                catch (Exception e)
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

            while (true)
            {
                try
                {
                    this.Log.Add(plmOS.Logging.Log.Levels.DEB, "Starting to download from SharePoint: " + this.URL);

                    using (Microsoft.SharePoint.Client.ClientContext SPContext = new Microsoft.SharePoint.Client.ClientContext(this.SiteURL.AbsoluteUri))
                    {
                        SPContext.Credentials = new Microsoft.SharePoint.Client.SharePointOnlineCredentials(this.Username, this.Password);

                        // Open Base Folder
                        Microsoft.SharePoint.Client.Folder SPBaseFolder = this.OpenBaseFolder(SPContext);

                        // Open Supplier Folder
                        Microsoft.SharePoint.Client.Folder SPSupplierFolder = this.OpenFolder(SPContext, SPBaseFolder, this.SupplierID);

                        // Open Project Folder
                        Microsoft.SharePoint.Client.Folder SPProjectFolder = this.OpenFolder(SPContext, SPSupplierFolder, this.ProjectID);

                        // Open Root Folder
                        Microsoft.SharePoint.Client.Folder SPRootFolder = this.OpenFolder(SPContext, SPProjectFolder, "Database");

                        // Get Listing of Transaction files on SharePoint
                        SPContext.Load(SPRootFolder.Files);
                        SPContext.ExecuteQuery();

                        // Create list of comitted Transactions
                        List<Int64> committedtransactions = new List<Int64>();

                        foreach (Microsoft.SharePoint.Client.File transactionfile in SPRootFolder.Files)
                        {
                            Int64 transactiondate = -1;

                            if (Path.GetExtension(transactionfile.Name).ToLower() == ".comitted")
                            {
                                if (Int64.TryParse(Path.GetFileNameWithoutExtension(transactionfile.Name), out transactiondate))
                                {
                                    if (!committedtransactions.Contains(transactiondate))
                                    {
                                        committedtransactions.Add(transactiondate);
                                    }
                                }
                            }
                        }

                        // Create List of Transactions that need to be Downloaded
                        List<Microsoft.SharePoint.Client.File> tobedownlaoded = new List<Microsoft.SharePoint.Client.File>();

                        foreach (Microsoft.SharePoint.Client.File transactionfile in SPRootFolder.Files)
                        {
                            Int64 transactiondate = -1;

                            if (Path.GetExtension(transactionfile.Name).ToLower() == ".zip")
                            {
                                if (Int64.TryParse(Path.GetFileNameWithoutExtension(transactionfile.Name), out transactiondate))
                                {
                                    if (committedtransactions.Contains(transactiondate))
                                    {
                                        if (!this.Downloaded.Contains(transactiondate))
                                        {
                                            tobedownlaoded.Add(transactionfile);
                                        }
                                    }
                                }
                            }
                        }

                        if (tobedownlaoded.Count > 0)
                        {
                            this.ReadingTotal = tobedownlaoded.Count;
                            this.ReadingNumber = 0;
                            this.Reading = true;

                            foreach (Microsoft.SharePoint.Client.File transactionfile in tobedownlaoded)
                            {
                                Int64 transactiondate = -1;

                                if (Int64.TryParse(Path.GetFileNameWithoutExtension(transactionfile.Name), out transactiondate))
                                {
                                    if (!this.Downloaded.Contains(transactiondate))
                                    {
                                        // Check if Transaction Folder Exists in Local Cache
                                        Boolean downloadneeded = true;

                                        DirectoryInfo localtransactionfolder = new DirectoryInfo(this.LocalRootFolder.FullName + "\\" + transactiondate.ToString());
                                        DirectoryInfo localtransactiontmpfolder = new DirectoryInfo(this.LocalRootFolder.FullName + "\\" + transactiondate.ToString() + ".download");
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
                                            // Download Transaction File from SharePoint
                                            FileInfo localtransactionfile = new FileInfo(this.LocalRootFolder.FullName + "\\" + transactionfile.Name);
                                            Microsoft.SharePoint.Client.FileInformation transactionfileInfo = Microsoft.SharePoint.Client.File.OpenBinaryDirect(SPContext, transactionfile.ServerRelativeUrl);

                                            using (FileStream sw = System.IO.File.OpenWrite(localtransactionfile.FullName))
                                            {
                                                using (transactionfileInfo.Stream)
                                                {
                                                    while ((sizeread = transactionfileInfo.Stream.Read(buffer, 0, buffersize)) > 0)
                                                    {
                                                        sw.Write(buffer, 0, sizeread);
                                                    }
                                                }
                                            }

                                            // Extract files from ZIP File
                                            if (!localtransactiontmpfolder.Exists)
                                            {
                                                localtransactiontmpfolder.Create();
                                            }
                                            else
                                            {
                                                foreach (FileInfo file in localtransactiontmpfolder.GetFiles())
                                                {
                                                    file.Delete();
                                                }
                                            }

                                            ZipFile.ExtractToDirectory(localtransactionfile.FullName, localtransactiontmpfolder.FullName);

                                            // Move XML Files to Transaction Directory
                                            foreach (FileInfo tmpxmlfile in localtransactiontmpfolder.GetFiles("*.xml"))
                                            {
                                                FileInfo xmlfile = new FileInfo(localtransactionfolder.FullName + "\\" + tmpxmlfile.Name);

                                                if (xmlfile.Exists)
                                                {
                                                    xmlfile.Delete();
                                                    xmlfile.Refresh();
                                                }

                                                tmpxmlfile.MoveTo(xmlfile.FullName);
                                            }

                                            // Move Vault Files to Vault
                                            foreach (FileInfo tmpvaultfile in localtransactiontmpfolder.GetFiles("*.dat"))
                                            {
                                                FileInfo vaultfile = new FileInfo(this.LocalVaultFolder.FullName + "\\" + tmpvaultfile.Name);

                                                if (vaultfile.Exists)
                                                {
                                                    vaultfile.Delete();
                                                    vaultfile.Refresh();
                                                }

                                                tmpvaultfile.MoveTo(vaultfile.FullName);
                                            }

                                            // Delete Temp Folder
                                            localtransactiontmpfolder.Delete(true);

                                            // Delete ZIP File
                                            localtransactionfile.Delete();

                                            // Create Committ File
                                            committed.Create();

                                            this.Downloaded.Add(transactiondate);
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
                            // Set to Initialised
                            this.Initialised = true;

                            // Start Upload
                            this.UploadThread = new Thread(this.Upload);
                            this.UploadThread.IsBackground = true;
                            this.UploadThread.Start();
                        }
                    }
                }
                catch (Exception e)
                {
                    this.Log.Add(plmOS.Logging.Log.Levels.ERR, "SharePoint download failed: " + e.Message);
                    this.Log.Add(plmOS.Logging.Log.Levels.DEB, "SharePoint download failed: " + e.Message + Environment.NewLine + e.StackTrace);
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
