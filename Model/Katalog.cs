
using Newtonsoft.Json;
using System.Collections.Generic;


namespace KatalogConverter.Model
{
    public class KatalogItem
    {
        public KatalogItem(string bezeichnung, string referenzNr, string von, string bis)
        {
            this.bezeichnung = bezeichnung;
            this.ReferenzNr = referenzNr;
            this.von = von;
            this.bis = bis;
        }

        public string bezeichnung { get; set; }
        public string ReferenzNr { get; set; }
        public string von { get; set; }
        public string bis { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }

    public class Katalog
    {
        public string katalogid { get; set; }
        public string lieferung { get; set; }

        private List<KatalogItem> katalog { get; set; }

        public Katalog(string katalogId, string lieferung)
        {
            this.katalogid = katalogId;
            this.lieferung = lieferung;
            katalog = new List<KatalogItem>();
        }

        public bool contains(string refid)
        {
            return katalog.Find(e => e.ReferenzNr == refid) != null;
        }

        public void add(KatalogItem item)
        {
            katalog.Add(item);
        }

        public override string ToString()
        {
            return "{" +
                "\"katalogid\":\"" + katalogid + "\"," +
                "\"lieferung\":\"" + lieferung + "\"," +
                "\"katalog\":\"" + JsonConvert.SerializeObject(this.katalog.ToArray())+"\"" +
                "}"; 
        }
    }
}
