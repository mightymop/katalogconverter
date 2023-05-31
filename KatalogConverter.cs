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
        private string convertFile;
        private string convertParentDir;
        private Newtonsoft.Json.Linq.JArray output;
        private string delimiter;

        private int col_katId;
        private int col_von;
        private int col_bis;
        private int col_refnr;
        private int col_bez;

        private string sourcefile;
        private string rolloutFile;
        private Timer lazyTimer;
        private Timer lazyTimerInit;

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
                
            }
            catch (Exception err)
            {
                log.Error("Fehler beim Initialisieren des Dienstes.", err);
                startLazyInit();
            }

          /*  try
            {
                this.testOutput();

            }
            catch (Exception err)
            {
                log.Error("Fehler beim Test des Outputs", err);
                startLazyInit();
            }*/
            
            try { 
                this.watch(this.watchDir);
                log.Info("Dienst überwacht Verzeichnis: " + this.watchDir);
            }
            catch (Exception err)
            {
                log.Error("Fehler beim Start des Dienstes.", err);
                startLazyInit();
            }
        }

     /*   private void testOutput()
        {
            foreach (string outputpath in this.output)
            {
                string outputlieferung = Path.Combine(outputpath, "test");
                if (!Directory.Exists(outputlieferung))
                {
                    Directory.CreateDirectory(outputlieferung);
                }

                string fullpath = Path.Combine(outputlieferung, "test.json");
                log.Debug("Schreibe Datei: " + fullpath);

                File.WriteAllText(fullpath, "{ \"Test\": \""+DateTime.Now.ToLocalTime()+"\"}", Encoding.UTF8);
            }
        }*/

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
            this.convertParentDir = jsonSettings.convert_parentdir;
            this.convertFile = jsonSettings.convert_filename;

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
                this.watcher = null;
            }
            catch (Exception e)
            {
                this.watcher = null;
                log.Error("Fehler beim Stoppen des Dienstes.", e);
            }
        }

        private void watch(string path)
        {
            log.Debug("Initialisiere FileSystemWatcher Objekt");
            this.watcher = new FileSystemWatcher();

            log.Debug("Setze Pfad: "+path);
            this.watcher.Path = path;

            log.Debug("setze Filter");
            this.watcher.NotifyFilter = NotifyFilters.Attributes
                                 | NotifyFilters.CreationTime
                                 | NotifyFilters.DirectoryName
                                 | NotifyFilters.FileName
                                 | NotifyFilters.LastAccess
                                 | NotifyFilters.LastWrite
                                 | NotifyFilters.Security
                                 | NotifyFilters.Size;

            log.Debug("setze onCreate Event");
            // watcher.Changed += OnChanged;
            this.watcher.Created += OnCreated;
            // watcher.Deleted += OnDeleted;
            //watcher.Renamed += OnRenamed;

            log.Debug("setze onError Event");
            this.watcher.Error += OnError;

            log.Debug("Watch for File with Filter: " + watchFile);
            this.watcher.Filter = watchFile; //"*.txt";

            log.Debug("Watch for subdirs");
            this.watcher.IncludeSubdirectories = true;

            log.Debug("Enable raising events");
            this.watcher.EnableRaisingEvents = true;

            log.Debug("set Buffsize");
            this.watcher.InternalBufferSize *= 8;

            log.Debug("Initialisierung abgeschlossen");
        }

        private void OnCreated(object sender, FileSystemEventArgs e)
        {
            try
            {
               
                FileInfo frolloutDate = new FileInfo(e.FullPath);
                log.Debug("OnCreated: "+ e.FullPath);

                string parentDir = Path.Combine(frolloutDate.Directory.FullName, this.convertParentDir);
                string convertFile = Path.Combine(parentDir, this.convertFile);

                FileInfo fConvertFile = new FileInfo(convertFile);

                log.Debug("fConvertFile: " + fConvertFile.FullName);
                log.Debug("parentDir: " + parentDir);

                if (fConvertFile.Exists && frolloutDate.Exists)
                {
                    log.Info("Es wurden eine neue Quelldatei (" + fConvertFile.FullName + ") erkannt. Starte Konvertierung.");
                    this.setSource(fConvertFile.FullName);
                    this.setRolloutFile(frolloutDate.FullName);
                    this.startLazyConvert();
                }
                else
                {
                    log.Error("Konvertierungsvorraussetzungen nicht erfüllt.");
                    log.Error("fConvertFile: " + fConvertFile.Exists);
                    log.Error("frolloutDate: " + frolloutDate.Exists);
                }
            }
            catch (Exception ex)
            {
                log.Error("Fehler beim Starten des LazyTimers.", ex);
            }
        }

        public void setRolloutFile(string rollout)
        {
            this.rolloutFile = rollout;
        }

        public void setSource(string source)
        {
            this.sourcefile = source;
        }

        private void OnError(object sender, ErrorEventArgs e) {
            log.Error("Fehler beim Erkennen auf Veränderungen im Dateisystem:",e.GetException());
            if (this.watcher != null)
            {
                this.watcher.Dispose();
            }
            this.watcher = null;
            this.startLazyInit();
        }

        private void startLazyInit()
        {
            this.lazyTimerInit = new Timer();
            this.lazyTimerInit.Elapsed += new ElapsedEventHandler(OnTimedInitEvent);
            this.lazyTimerInit.Interval = 1000*60*10;
            log.Info("Starte Init Timer...");
            this.lazyTimerInit.Start();
        }

        private void OnTimedInitEvent(object source, ElapsedEventArgs e)
        {
            this.lazyTimerInit.Stop();
            this.lazyTimerInit = null;
            log.Info("Dienst wird gestartet.");
            try
            {
                this.init();
                this.watch(this.watchDir);
                log.Info("Dienst überwacht Verzeichnis: " + this.watchDir);
            }
            catch (Exception err)
            {
                log.Error("Fehler beim Start des Dienstes.", err);
                if (this.watcher != null)
                {
                    this.watcher.Dispose();
                }
                this.watcher = null;
                startLazyInit();
            }
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
            if (File.Exists(this.rolloutFile))
            {
                string rolloutDate = File.ReadAllText(this.rolloutFile, Encoding.UTF8);
                this.convert(this.sourcefile, this.delimiter, this.output, rolloutDate);
            }
            else
            {
                log.Warn("Rolloutdatei wurde vor Beginn der Konvertierung gelöscht. Vorgang abgebrochen.");
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
                        string fullpath = Path.Combine(outputlieferung, Convert.ToString(key) + ".json");
                        log.Debug("Schreibe Datei: " + fullpath);

                        File.WriteAllText(fullpath, kataloge[key].ToString(), Encoding.UTF8);
                    }
                }
            }
            log.Info("Fertig.");
        }

        
    }
}
