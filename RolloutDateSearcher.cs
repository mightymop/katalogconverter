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

        public void SearchLatestRolloutDate(string rootDirectory, out string dir, out string date)
        {
            dir = null;
            date = null;

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
