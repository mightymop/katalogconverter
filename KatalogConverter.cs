using KatalogConverter.Model;
using log4net;
using log4net.Config;
using Microsoft.VisualBasic.FileIO;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Timers;

namespace KatalogConverter
{
    public partial class KatalogConverter : ServiceBase
    {
        private FileSystemWatcher watcher;
        private ILog log = LogManager.GetLogger(typeof(KatalogConverter));
        private string watchDir;
        private string watchFile;
        private string watchParentDir;
        private Newtonsoft.Json.Linq.JArray output;
        private string delimiter;

        private int col_katId;
        private int col_von;
        private int col_bis;
        private int col_refnr;
        private int col_bez;

        private int errStartCount = 0;

        private string sourcefile;
        private Timer lazyTimer;

        private static string ROLLOUTTXT = "rolloutDate.txt";

        public KatalogConverter()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            log.Info("Dienst wird gestartet.");
            try
            {
                this.init();
                this.watch(this.watchDir);
                log.Info("Dienst überwacht Verzeichnis: "+this.watchDir);
            }
            catch (Exception e)
            {
                log.Error("Fehler beim Start des Dienstes.",e);
            }
        }

        public void init()
        {
            string location = AppDomain.CurrentDomain.BaseDirectory;//System.Reflection.Assembly.GetEntryAssembly().Location;
            string workdir = location.EndsWith(".exe") ? location.Substring(0, location.LastIndexOf(Path.PathSeparator)) : location;
            Directory.SetCurrentDirectory(workdir);
            XmlConfigurator.Configure(new FileInfo("log4net.config"));
            log.Info("Arbeitsverzeichnis: " + workdir);

            dynamic jsonSettings = JsonConvert.DeserializeObject(System.IO.File.ReadAllText("appsettings.json"));
            this.watchDir = jsonSettings.watch_directory;
            this.watchFile = jsonSettings.watch_filename;
            this.watchParentDir = jsonSettings.watch_parentdir;
            this.output = jsonSettings.output;
            this.delimiter = jsonSettings.delimiter;

            this.col_katId = jsonSettings.col_katId is string ? Convert.ToInt32(jsonSettings.col_katId) : jsonSettings.col_katId;
            this.col_von = jsonSettings.col_von is string ? Convert.ToInt32(jsonSettings.col_von) : jsonSettings.col_von;
            this.col_bis = jsonSettings.col_bis is string ? Convert.ToInt32(jsonSettings.col_bis) : jsonSettings.col_bis;
            this.col_refnr = jsonSettings.col_refnr is string ? Convert.ToInt32(jsonSettings.col_refnr) : jsonSettings.col_refnr;
            this.col_bez = jsonSettings.col_bez is string ? Convert.ToInt32(jsonSettings.col_bez) : jsonSettings.col_bez;
        }

        protected override void OnStop()
        {
            try
            {
                log.Info("Dienst wird beendet.");
                if (this.watcher != null)
                {
                    this.watcher.Dispose();
                }
            }
            catch (Exception e)
            {
                log.Error("Fehler beim Start des Dienstes.", e);
            }
        }

        private void watch(string path)
        {
            this.watcher = new FileSystemWatcher(path);

            this.watcher.NotifyFilter = NotifyFilters.Attributes
                                 | NotifyFilters.CreationTime
                                 | NotifyFilters.DirectoryName
                                 | NotifyFilters.FileName
                                 | NotifyFilters.LastAccess
                                 | NotifyFilters.LastWrite
                                 | NotifyFilters.Security
                                 | NotifyFilters.Size;

            // watcher.Changed += OnChanged;
            this.watcher.Created += OnCreated;
            // watcher.Deleted += OnDeleted;
            //watcher.Renamed += OnRenamed;
            this.watcher.Error += OnError;

            this.watcher.Filter = watchFile; //"*.txt";
            this.watcher.IncludeSubdirectories = true;
            this.watcher.EnableRaisingEvents = true;
            this.watcher.InternalBufferSize *= 8;
        }

        private void OnCreated(object sender, FileSystemEventArgs e)
        {
            try
            {
                FileInfo fi = new FileInfo(e.FullPath);
                if (fi.DirectoryName.ToLower().Equals(this.watchParentDir.ToLower()))
                {
                    log.Info("Es wurden eine neue Quelldatei (" + e.FullPath + ") erkannt. Starte Konvertierung.");
                    this.setSource(e.FullPath);
                    this.startLazyConvert();
                }
            }
            catch (Exception ex)
            {
                log.Error("Fehler beim Starten des LazyTimers.", ex);
            }
        }

        public void setSource(string source)
        {
            this.sourcefile = source;
        }

        private void OnError(object sender, ErrorEventArgs e) {
            log.Error("Fehler beim Erkennen auf Veränderungen im Dateisystem:",e.GetException());
        }

        private void startLazyConvert()
        {
            this.lazyTimer = new Timer();
            this.lazyTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
            this.lazyTimer.Interval = 30000;
            log.Info("Beginne Datenkonvertierung in 30Sek.");
            this.lazyTimer.Start();   
        }

        private void OnTimedEvent(object source, ElapsedEventArgs e)
        {
            try
            {
                this.lazyTimer.Stop();
                this.doWork();
            }
            catch (Exception ex)
            {
                log.Error("Fehler beim Konvertieren der Daten.", ex);
            }
        }

        public void doWork()
        {
            FileInfo fileInfo = new FileInfo(this.sourcefile);
            String txtFile = Path.Combine(fileInfo.Directory.Parent.FullName, ROLLOUTTXT);
            if (File.Exists(txtFile))
            {
                string rolloutDate = File.ReadAllText(txtFile, Encoding.UTF8);
                this.convert(this.sourcefile, this.delimiter, this.output, rolloutDate);
            }
            else
            {
                this.errStartCount++;
                if (this.errStartCount > 10)
                {
                    log.Error("Abbruch der Konvertierung: Konnte die Rollout TXT Datei nicht finden!");
                    return;
                }
                this.startLazyConvert();
            }
        }

        /*
         PLXKRDS1 = GESAMT DATENBANK (CSV)

        QUELLE:

            KAT_ID,?,?,VON,BIS,ID,BEZEICHNUNG

        ZIEL:

            {
                "katalogid": "KAT_ID",
                "lieferung": "DATEI_DATUM",
                "katalog": [{ "bezeichnung": "BEZEICHNUNG", "ReferenzNr": "ID", "von": "VON", "bis": "BIS" }]
            }

        */

        public string strip(string data, Encoding enc)
        {
            Encoding utf8 = Encoding.UTF8;
            byte[] bytes = enc.GetBytes(data);
            byte[] encBytes = Encoding.Convert(enc, utf8, bytes);
            data = utf8.GetString(encBytes);

            return data.Trim().StartsWith("'") && data.Trim().EndsWith("'") ? data.Substring(1, data.Length - 2) : data;
        }

        public Encoding GetEncoding(string filename)
        {
            using (var reader = new StreamReader(filename, Encoding.Default, true))
            {
                if (reader.Peek() >= 0) // you need this!
                    reader.Read();

                return reader.CurrentEncoding;
            }
        }

        public void convert(string file, string delimiter, Newtonsoft.Json.Linq.JArray outputpaths, string rolloutdate) //PLXKRDS2, PLXKRDS4
        {
            Dictionary<int, Katalog> kataloge = new Dictionary<int, Katalog>();

            log.Info("Lese Datei: " + file);
            Encoding enc = GetEncoding(file);
          
            using (TextFieldParser parser = new TextFieldParser(file, enc))
            {
                parser.TextFieldType = FieldType.Delimited;
                log.Info("Verwende Delimiter: '" + delimiter + "'");
                parser.SetDelimiters(delimiter);

                log.Info("Spaltenindex Katalogid: "+Convert.ToString(this.col_katId));
                log.Info("Spaltenindex von: " + Convert.ToString(this.col_von));
                log.Info("Spaltenindex bis: " + Convert.ToString(this.col_bis));
                log.Info("Spaltenindex ReferenzNr: " + Convert.ToString(this.col_refnr));
                log.Info("Spaltenindex Bezeichnung: " + Convert.ToString(this.col_bez));
             
                string aktKatalogID = null;
               
                log.Info("Lieferdatum: " + rolloutdate);

                
                log.Info("Source-Encoding: " + enc.EncodingName);
                log.Info("Target-Encoding: UTF-8");
                log.Debug("Bitte warten...");

                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();

                    string katalogid = strip(fields[col_katId], enc);
                    string von = strip(fields[col_von], enc);
                    string bis = strip(fields[col_bis], enc);
                    string refid = strip(fields[col_refnr], enc);
                    string bezeichnung = strip(fields[col_bez], enc);

                    Katalog aktKatalog;

                    if (aktKatalogID == null || (aktKatalogID != katalogid && kataloge.ContainsKey(Convert.ToInt32(katalogid)) == false))
                    {
                        aktKatalogID = katalogid;
                        aktKatalog = new Katalog(katalogid, rolloutdate);
                        kataloge.Add(Convert.ToInt32(aktKatalogID), aktKatalog);
                    }
                    else
                    {
                        aktKatalog = kataloge[Convert.ToInt32(katalogid)];
                    }

                    if (!aktKatalog.contains(refid))
                    {
                        aktKatalog.add(new KatalogItem(bezeichnung, refid, von, bis));
                    }
                }
            }

            if (kataloge.Count > 0)
            {
                var list = kataloge.Keys.ToList();
                list.Sort();

                log.Info("Anzahl Kataloge: " + Convert.ToString(kataloge.Count));
                
                foreach (string outputpath in outputpaths)
                {
                    string outputlieferung = Path.Combine(outputpath, rolloutdate);
                    if (!Directory.Exists(outputlieferung))
                    {
                        Directory.CreateDirectory(outputlieferung);
                    }

                    foreach (int key in list)
                    {
                        log.Debug("Schreibe Datei: " + Convert.ToString(key) + ".json");

                        File.WriteAllText(Path.Combine(outputlieferung, Convert.ToString(key) + ".json"), kataloge[key].ToString(), Encoding.UTF8);
                    }
                }
            }
            log.Info("Fertig.");
        }

        
    }
}
