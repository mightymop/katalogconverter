using System;
using System.Configuration.Install;
using System.Reflection;
using System.Security.Principal;
using System.ServiceProcess;


namespace KatalogConverter
{
    internal static class Program
    {
        /// <summary>
        /// Der Haupteinstiegspunkt für die Anwendung.
        /// </summary>
        static void Main(string[] args)
        {
            if (args == null || args.Length == 0)
            {
                Console.WriteLine("Starte Dienst");
                ServiceBase[] ServicesToRun;
                ServicesToRun = new ServiceBase[]
                {
                 new KatalogConverter()
                };
                ServiceBase.Run(ServicesToRun);
            }
            else
            if (args.Length == 1 && (args[0]=="i" || args[0]=="install")&& IsAdministrator())
            {
                Console.WriteLine("Installiere Dienst");
                ManagedInstallerClass.InstallHelper(new string[] { Assembly.GetExecutingAssembly().Location });
            }
            else
            if (args.Length == 1 && (args[0] == "r" || args[0] == "remove") && IsAdministrator())
            {
                Console.WriteLine("Entferne Dienst");
                ManagedInstallerClass.InstallHelper(new string[] { "/u", Assembly.GetExecutingAssembly().Location });
            }
            else
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("PROGRAMM [i|r|help]");
                Console.WriteLine("i | install = Dienst installieren");
                Console.WriteLine("r | remove = Dienst entfernen");
                Console.WriteLine("h | help  = Diese Hilfe anzeigen.");
                Console.WriteLine("*ohne Parameter wird der Dienst ausgeführt.");
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
