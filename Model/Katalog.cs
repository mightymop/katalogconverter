
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace KatalogConverter.Model
{
    public class KatalogItem
    {
        public KatalogItem(string displayValue, string referenceId, string validFrom, string validUntil)
        {
            this.displayValue = displayValue;
            this.referenceId = referenceId;
            this.validFrom = ValidateAndConvertDateFormat(validFrom);
            this.validUntil = ValidateAndConvertDateFormat(validUntil);
        }

        public string displayValue { get; set; }
        public string referenceId { get; set; }
        public string validFrom { get; set; }
        public string validUntil { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

        private string ValidateAndConvertDateFormat(string date)
        {
            // Überprüfen, ob das Datum dem Format "yyyy-mm-dd" entspricht
            if (DateTime.TryParseExact(date, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            {
                return date; // Wenn das Format bereits korrekt ist, beibehalten
            }
            else
            {
                // Wenn das Format "dd.mm.yyyy" ist, dann umwandeln
                if (DateTime.TryParseExact(date, "dd.MM.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime convertedDate))
                {
                    return convertedDate.ToString("yyyy-MM-dd");
                }
                else
                {
                    // Wenn weder das eine noch das andere Format gültig ist, Fehlerbehandlung hier hinzufügen
                    throw new ArgumentException("Ungültiges Datumsformat");
                }
            }
        }
    }

    public class Katalog
    {
        public string katalogid { get; set; }
        public string katalogname { get; set; }
        public string lieferung { get; set; }

        private List<KatalogItem> katalog { get; set; }

        public Katalog(string katalogId, string katalogName, string lieferung)
        {
            this.katalogid = katalogId;
            this.lieferung = lieferung;
            this.katalogname = ConvertSpecialCharacters(katalogName);
            katalog = new List<KatalogItem>();
        }

        public bool contains(string refid)
        {
            return katalog.Find(e => e.referenceId == refid) != null;
        }

        public void add(KatalogItem item)
        {
            katalog.Add(item);
        }

        public static string ConvertSpecialCharacters(string inputString)
        {
            string convertedString = inputString
                .Replace("ä", "ae")
                .Replace("ö", "oe")
                .Replace("ü", "ue")
                .Replace("Ä", "Ae")
                .Replace("Ö", "Oe")
                .Replace("Ü", "Ue")
                .Replace("ß", "ss")
                .Replace("KAT_KAT_", "KAT_")
                .Replace("-", "_");

            return convertedString.ToUpper();
        }

        public override string ToString()
        {
            return "{" +
                "\"katalogid\":\"" + katalogid + "\"," +
                "\"katalogname\":\"" + katalogname + "\"," +
                "\"lieferung\":\"" + lieferung + "\"," +
                "\"katalog\":" + JsonConvert.SerializeObject(this.katalog.ToArray())+
                "}"; 
        }
    }
}
