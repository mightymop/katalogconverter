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
using System.Threading;
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
        private string convert_katdic;
        private string convert_versionfile;
        private string convertParentDir;
        private Newtonsoft.Json.Linq.JArray output;
        private string delimiter;
        private bool check_filecontent;

        private int col_katId;
        private int col_von;
        private int col_bis;
        private int col_refnr;
        private int col_bez;

        private int col_katdic_katId;
        private int col_katdic_katName;

        private int col_katversion;
        private int col_katdate;

        private string sourcefile;
        private string sourceKatDic;
        private string sourceVersionfile;
        private string rolloutFile;
        private System.Timers.Timer lazyTimer;
        private System.Timers.Timer lazyTimerInit;

        private System.Threading.Timer restartTimer;

        public KatalogConverter()
        {
            InitializeComponent();
        }

        private void StartRestartTimer()
        {
            // Timer erstellen, der alle 18 Stunden ausgelöst wird (18 Stunden * 60 Minuten * 60 Sekunden * 1000 Millisekunden)
            restartTimer = new System.Threading.Timer(RestartService, null, TimeSpan.FromHours(18), Timeout.InfiniteTimeSpan);
        }

        private void StopRestartTimer()
        {
            // Timer stoppen
            restartTimer?.Change(Timeout.Infinite, Timeout.Infinite);
            restartTimer?.Dispose();
        }

        private void RestartService(object state)
        {
            log.Info("Dienst wird neu gestartet.");
            OnStop();
            OnStart(null);
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
            
            try { 
                this.watch(this.watchDir);
                log.Info("Dienst überwacht Verzeichnis: " + this.watchDir);

                RolloutDateSearcher searcher = new RolloutDateSearcher();
                   
                string latestDate = "";
                string latestDir = "";
                string lDate = null;


                try
                {
                    // StreamReader instanziieren
                    using (StreamReader reader = new StreamReader("latest.txt"))
                    {
                        // Dateiinhalt lesen und etwas damit machen
                        log.Info("Dienst sucht nach aktueller rolloutDate.txt Datei.");
                        string inhalt = reader.ReadToEnd();
                        lDate = inhalt;
                        log.Info("Datum: " + lDate);
                        reader.Close();
                    }
                }
                catch (Exception ex)
                {
                    // Fehlerbehandlung, falls etwas schief geht
                    log.Error("Fehler beim Lesen der Datei: " + ex.Message);
                }
               
                searcher.SearchLatestRolloutDate(this.watchDir, this.check_filecontent, lDate, out latestDir, out latestDate);

                if (!string.IsNullOrEmpty(latestDir))
                {
                    log.Info("Neueres Datum gefunden.");
                
                    string parentDir = Path.Combine(latestDir, this.convertParentDir);
                    string convertFile = Path.Combine(parentDir, this.convertFile);
                    string convert_katdic = Path.Combine(parentDir, this.convert_katdic);
                    string convert_versionfile = Path.Combine(parentDir, this.convert_versionfile);

                    this.convert(convertFile, convert_katdic, convert_versionfile, this.delimiter, this.output, latestDate);

                    try
                    {
                        File.WriteAllText("latest.txt", latestDate);
                        log.Debug("Datum wurde erfolgreich in die Datei geschrieben.");
                    }
                    catch (Exception ex)
                    {
                        log.Warn("Fehler beim Schreiben der Datei: " + ex.Message);
                    }
                }
                else
                {
                    log.Info("Kein neueres Datum gefunden.");
                }
                
            }
            catch (Exception err)
            {
                log.Error("Fehler beim Start des Dienstes.", err);
                startLazyInit();
            }
        }


        public void init()
        {
            StartRestartTimer();

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
            this.convert_katdic = jsonSettings.convert_katdic;
            this.convert_versionfile = jsonSettings.convert_versionfile;

            this.check_filecontent = jsonSettings.check_filecontent;

            this.output = jsonSettings.output;
            this.delimiter = jsonSettings.delimiter;

            this.col_katId = jsonSettings.col_katId is string ? Convert.ToInt32(jsonSettings.col_katId) : jsonSettings.col_katId;
            this.col_von = jsonSettings.col_von is string ? Convert.ToInt32(jsonSettings.col_von) : jsonSettings.col_von;
            this.col_bis = jsonSettings.col_bis is string ? Convert.ToInt32(jsonSettings.col_bis) : jsonSettings.col_bis;
            this.col_refnr = jsonSettings.col_refnr is string ? Convert.ToInt32(jsonSettings.col_refnr) : jsonSettings.col_refnr;
            this.col_bez = jsonSettings.col_bez is string ? Convert.ToInt32(jsonSettings.col_bez) : jsonSettings.col_bez;

            col_katdic_katId = jsonSettings.col_katdic_katId is string ? Convert.ToInt32(jsonSettings.col_katdic_katId) : jsonSettings.col_katdic_katId;
            col_katdic_katName = jsonSettings.col_katdic_katName is string ? Convert.ToInt32(jsonSettings.col_katdic_katName) : jsonSettings.col_katdic_katName;

            col_katversion = jsonSettings.col_katversion is string ? Convert.ToInt32(jsonSettings.col_katversion) : jsonSettings.col_katversion;
            col_katdate = jsonSettings.col_katdate is string ? Convert.ToInt32(jsonSettings.col_katdate) : jsonSettings.col_katdate;
        }

        protected override void OnStop()
        {
            try
            {
                log.Info("Dienst wird beendet.");

                StopRestartTimer();

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
                    string directoryPath = fConvertFile.Directory.FullName;                    
                    string newFilePathKatdic = Path.Combine(directoryPath, this.convert_katdic);
                    string newFilePathVersion = Path.Combine(directoryPath, this.convert_versionfile);
                    this.setKatDic(newFilePathKatdic);
                    this.setVersionFile(newFilePathVersion);
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

        public void setKatDic(string katDic)
        {
            this.sourceKatDic= katDic;
        }

        public void setVersionFile(string versionFile)
        {
            this.sourceVersionfile = versionFile;
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
            this.lazyTimerInit = new System.Timers.Timer();
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
            this.lazyTimer = new System.Timers.Timer();
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
               
                this.convert(this.sourcefile,this.sourceKatDic,this.sourceVersionfile,this.delimiter, this.output, rolloutDate);
                try
                {
                    File.Delete("latest.txt");
                    File.WriteAllText("latest.txt", rolloutDate);
                    log.Debug("Datum wurde erfolgreich in die Datei geschrieben.");
                }
                catch (Exception ex)
                {
                    log.Warn("Fehler beim Schreiben der Datei: " + ex.Message);
                }
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

                Encoding result = reader.CurrentEncoding;
                reader.Close();
                return result;
            }
        }

        public void writeKatInfofile(Dictionary<string,string> dictionary, string version, string date, string filePath)
        {            
            Dictionary<string,string> name_id = new Dictionary<string,string>();
            Dictionary<string, string> id_name= new Dictionary<string, string>();
            Dictionary<string, string> name_orig = new Dictionary<string, string>();

            foreach (var kvp in dictionary)
            {
                if (!id_name.ContainsKey(kvp.Key))
                {
                    try 
                    { 
                        id_name.Add(kvp.Key, Katalog.ConvertSpecialCharacters(kvp.Value).ToUpper());
                    }
                    catch (Exception e)
                    {
                        log.Error($"{kvp.Key} - {Katalog.ConvertSpecialCharacters(kvp.Value).ToUpper()} (key) nicht hinzugefügt: "+e.Message);
                    }
                }
                else
                {
                    log.Error($"{kvp.Key} - {Katalog.ConvertSpecialCharacters(kvp.Value).ToUpper()} (key) bereits vorhanden!");
                }

                if (!name_id.ContainsKey(Katalog.ConvertSpecialCharacters(kvp.Value).ToUpper()))
                {
                    try
                    {
                        name_id.Add(Katalog.ConvertSpecialCharacters(kvp.Value).ToUpper(), kvp.Key);
                    }
                    catch (Exception e)
                    {
                        log.Error($"{Katalog.ConvertSpecialCharacters(kvp.Value).ToUpper()} - {kvp.Key} (value) nicht hinzugefügt: " + e.Message);
                    }
                }
                else
                {
                    log.Error($"{Katalog.ConvertSpecialCharacters(kvp.Value).ToUpper()} - {kvp.Key} (value) bereits vorhanden!");
                }                
            }

            var jsonData = new
            {
                name_id = name_id,
                id_name = id_name,
                version = version,
                date = date
            };

            // Konvertieren des Objekts in JSON-Format
            string json = JsonConvert.SerializeObject(jsonData, Formatting.Indented);

            // Speichern des JSON in einer Datei
            File.WriteAllText(filePath, json);
        }

        public void convert(string file, string fileKatDic, string fileVersion, string delimiter, Newtonsoft.Json.Linq.JArray outputpaths, string rolloutdate) //PLXKRDS2, PLXKRDS4
        {
            Dictionary<string, Katalog> kataloge = new Dictionary<string, Katalog>();
            Dictionary<string, string> katdic = new Dictionary<string, string>();

            string version=null;
            string date=null;

            log.Info("Lese Datei: " + fileVersion);
            Encoding encVersion = GetEncoding(fileVersion);
            using (TextFieldParser parser = new TextFieldParser(fileVersion, encVersion))
            {
                parser.TextFieldType = FieldType.Delimited;
                log.Info("Verwende Delimiter: '" + delimiter + "'");
                parser.SetDelimiters(delimiter);

                while (!parser.EndOfData)
                {
                    if (version == null)
                    {
                        string[] fields = parser.ReadFields();
                        version = strip(fields[col_katversion], encVersion);
                        date = strip(fields[col_katdate], encVersion);
                        break;
                    }
                }

                parser.Close();
            }

            log.Info("Lese Datei: " + fileKatDic);
            Encoding encKat = GetEncoding(fileKatDic);
            using (TextFieldParser parser = new TextFieldParser(fileKatDic, encKat))
            {
                parser.TextFieldType = FieldType.Delimited;
                log.Info("Verwende Delimiter: '" + delimiter + "'");
                parser.SetDelimiters(delimiter);
                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();
                    string katid = strip(fields[col_katdic_katId], encKat);
                    string katname = strip(fields[col_katdic_katName], encKat);

                    if (!katdic.ContainsKey(katid))
                    {
                        katdic.Add(katid, katname);
                    }
                }

                parser.Close();
            }

      
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

                    string katalogName = katdic.ContainsKey(katalogid)?katdic[katalogid]:"";

                    Katalog aktKatalog;

                    if (aktKatalogID == null || (aktKatalogID != katalogid && kataloge.ContainsKey(katalogid) == false))
                    {
                        aktKatalogID = katalogid;
                        aktKatalog = new Katalog(katalogid,katalogName, rolloutdate);
                        kataloge.Add(aktKatalogID, aktKatalog);
                    }
                    else
                    {
                        aktKatalog = kataloge[katalogid];
                    }

                    if (!aktKatalog.contains(refid))
                    {
                        aktKatalog.add(new KatalogItem(bezeichnung, refid, von, bis));
                    }
                }

                parser.Close();
            }

            if (kataloge.Count > 0)
            {
                var list = kataloge.Keys.ToList();
                list.Sort();

                log.Info("Anzahl Kataloge: " + Convert.ToString(kataloge.Count));

                string temppath =  Path.Combine(Environment.CurrentDirectory, "temp");
                temppath = Path.Combine(temppath, rolloutdate);
                if (!Directory.Exists(temppath))
                {
                    Directory.CreateDirectory(temppath);
                }


                foreach (string key in list)
                {
                    if (!katdic.ContainsKey(key))
                    {
                        katdic.Remove(key);
                    }                   
                }

                string fullpathKatDic = Path.Combine(temppath, "dictionary.json");
                writeKatInfofile(katdic, version, date, fullpathKatDic);

                foreach (string key in list)
                {
                    string fullpath = Path.Combine(temppath, Convert.ToString(key) + ".json");
                    log.Debug("Schreibe Datei: " + fullpath);

                    File.WriteAllText(fullpath, kataloge[key].ToString(), Encoding.UTF8);
                }

                string[] files = Directory.GetFiles(temppath);

                foreach (string outputpath in outputpaths)
                {
                    string outputlieferung = Path.Combine(outputpath, version);
                    if (!Directory.Exists(outputlieferung))
                    {
                        Directory.CreateDirectory(outputlieferung);
                    }

                    foreach (string filePath in files)
                    {
                        // Den Dateinamen aus dem vollständigen Pfad extrahieren
                        string fileName = Path.GetFileName(filePath);

                        // Den vollständigen Ziel-Pfad erstellen
                        string destinationPath = Path.Combine(outputlieferung, fileName);

                        // Die Datei kopieren                        
                        File.Copy(filePath, destinationPath, true); // Der dritte Parameter (true) überschreibt die Datei, falls sie bereits existiert
                    }
                }
            }
            log.Info("Fertig.");
        }

        
    }
}
