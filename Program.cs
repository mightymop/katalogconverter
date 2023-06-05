using log4net;
using log4net.Config;
using System;
using System.Configuration.Install;
using System.IO;
using System.Reflection;
using System.Security.Principal;
using System.ServiceProcess;



namespace KatalogConverter
{
    class Program
    {
        private static ILog log = LogManager.GetLogger(typeof(Program));

        /// <summary>
        /// Der Haupteinstiegspunkt für die Anwendung.
        /// </summary>
        static void Main(string[] args)
        {
            XmlConfigurator.Configure(new FileInfo("log4net.config"));
            string test = "C:\\KatalogDienst\\TEST\\1\\rolloutDate.txt.txt".Replace(".txt.txt", ".txt");

            log.Info(args != null && args.Length > 0 ? "Parameter: " + String.Join(" ", args) : "Keine Parameter erkannt");

            if (args == null || args.Length == 0)
            {
                try
                {
                    log.Info("Starte Dienst");
                    ServiceBase[] ServicesToRun;
                    ServicesToRun = new ServiceBase[]
                    {
                        new KatalogConverter()
                    };
                    ServiceBase.Run(ServicesToRun);
                }
                catch (Exception ex) {
                    log.Error("Dienst konnte nicht gestartet werden: "+ex.Message);
                }
            }
            else
            if (args.Length == 1 && (args[0]=="i" || args[0]=="install")&& IsAdministrator())
            {
                log.Info("Installiere Dienst");
                ManagedInstallerClass.InstallHelper(new string[] { Assembly.GetExecutingAssembly().Location });
            }
            else
            if (args.Length == 1 && (args[0] == "r" || args[0] == "remove") && IsAdministrator())
            {
                log.Info("Entferne Dienst");
                ManagedInstallerClass.InstallHelper(new string[] { "/u", Assembly.GetExecutingAssembly().Location });
            }
            else
            {
                log.Info("Usage:");
                log.Info("PROGRAMM [i|r|help]");
                log.Info("i | install = Dienst installieren");
                log.Info("r | remove = Dienst entfernen");
                log.Info("h | help  = Diese Hilfe anzeigen.");
                log.Info("*ohne Parameter wird der Dienst ausgeführt.");
                log.Info("*nicht vergessen, den User für \"Login as a service\" zu berechtigen!!!");
                log.Info("> https://learn.microsoft.com/de-de/troubleshoot/windows-server/system-management-components/service-startup-permissions");
            }

        }

        public static bool IsAdministrator()
        {
            var identity = WindowsIdentity.GetCurrent();
            var principal = new WindowsPrincipal(identity);
            return principal.IsInRole(WindowsBuiltInRole.Administrator);
        }
    }
}
