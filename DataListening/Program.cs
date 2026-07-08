using System;
using System.Globalization;
using System.Threading;

namespace TriIPTAIAPI
{
    class Program
    {
        static void Main(string[] args)
        {
            // Use invariant culture to keep numeric formatting consistent.
            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;

            Console.WriteLine("=== Tri-IPTA Real-Time MS/MS Triggering ===");
            Console.WriteLine();

            bool enableMS2 = false;

            // Parse command-line arguments.
            if (args.Length > 0)
            {
                foreach (var arg in args)
                {
                    if (arg.Equals("-ms2", StringComparison.OrdinalIgnoreCase))
                    {
                        enableMS2 = true;
                        break;
                    }
                }
            }
            else
            {
                // Interactive mode selection.
                Console.WriteLine("Select acquisition-control mode:");
                Console.WriteLine("1. MS1 triplet detection only");
                Console.WriteLine("2. MS1 triplet detection + dynamic MS/MS triggering");
                Console.Write("\nEnter choice (1 or 2): ");

                var choice = Console.ReadLine()?.Trim();
                enableMS2 = (choice == "2");

                if (choice != "1" && choice != "2")
                {
                    Console.WriteLine("[WARN] Invalid selection. Using mode 1: MS1 triplet detection only.");
                    enableMS2 = false;
                }
            }

            if (enableMS2)
            {
                Console.WriteLine("\n[INFO] Mode: MS1 triplet detection + dynamic MS/MS triggering");
                Console.WriteLine("[INFO] MS/MS events are controlled through the instrument inclusion list.");

                Console.Write("Press Enter to confirm, ESC to cancel: ");
                var key = Console.ReadKey(true);

                if (key.Key == ConsoleKey.Escape)
                {
                    Console.WriteLine("\n[INFO] Run cancelled by user.");
                    return;
                }
            }
            else
            {
                Console.WriteLine("\n[INFO] Mode: MS1 triplet detection only");
                Console.WriteLine("Press Enter to continue...");
                Console.ReadLine();
            }

            Console.WriteLine("\n[INFO] Starting Tri-IPTA acquisition monitor...");

            try
            {
                var detector = new TriIptaTripletDetector(enableMS2);
                detector.DoJob();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] {ex.Message}");
            }

            Console.WriteLine("\n[INFO] Program finished. Press any key to exit...");
            Console.ReadKey();
        }
    }
}
