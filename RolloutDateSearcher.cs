using log4net;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KatalogConverter
{
    public class RolloutDateSearcher
    {
        private string latestDirectory;
        private ILog log = LogManager.GetLogger(typeof(RolloutDateSearcher));

        private string filename = "rolloutDate.txt";
        private string latestDate;
        private bool check_filecontent;

        public void SearchLatestRolloutDate(string rootDirectory, bool check_filecontent, string lDate, out string dir, out string date)
        {
            dir = null;
            date = null;
            latestDate = lDate != null ? lDate : null;
            this.check_filecontent = check_filecontent;
            if (!Directory.Exists(rootDirectory))
            {
                log.Error("Das angegebene Verzeichnis existiert nicht.");
                return;
            }

            SearchDirectory(rootDirectory);

            if (!string.IsNullOrEmpty(latestDate))
            {
                log.Debug($"Neuestes Datum: {latestDate}");
                log.Debug($"Verzeichnis: {latestDirectory}");
                dir = latestDirectory;
                date = latestDate;
            }
            else
            {
                log.Warn("Keine Datei "+ filename + " gefunden.");
            }
        }

        private void SearchDirectory(string directory)
        {
            try
            {
                foreach (string file in Directory.GetFiles(directory, filename))
                {
                    string date = ReadRolloutDateFromFile(file);
                    if (IsNewerDate(date, latestDate))
                    {
                        latestDate = date;
                        latestDirectory = directory;
                    }
                }

                foreach (string subDir in Directory.GetDirectories(directory))
                {
                    SearchDirectory(subDir);
                }
            }
            catch (Exception ex)
            {
                // Handle any exceptions here (e.g., access denied, etc.)
                log.Error("Fehler beim Durchsuchen des Verzeichnisses: " + ex.Message);
            }
        }

        public string ReadRolloutDateFromFile(string filePath)
        {
            if (check_filecontent)
            {
                try
                {
                    return File.ReadAllText(filePath).Trim();
                }
                catch (Exception ex)
                {
                    // Handle any exceptions here (e.g., file not found, etc.)
                    log.Error("Fehler beim Lesen der Datei: " + ex.Message);
                    return null;
                }
            }
            else
            {
                FileInfo rolloutFileInfo = new FileInfo(filePath);
                return rolloutFileInfo.CreationTime.ToString("dd.MM.yyyy");
            }
        }

        public bool IsNewerDate(string date, string latestDate)
        {
            if (string.IsNullOrEmpty(date)) return false;

            if (DateTime.TryParse(date, out DateTime parsedDate))
            {
                if (DateTime.TryParse(latestDate, out DateTime latestParsedDate))
                {
                    return parsedDate > latestParsedDate;
                }
                return true;
            }
            return false;
        }
    }
}
