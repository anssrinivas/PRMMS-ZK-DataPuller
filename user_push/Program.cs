using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Google.Protobuf.WellKnownTypes;
using System.Text;
using MySql.Data.MySqlClient;
using System.Configuration;
using log4net;
using log4net.Config;
using System.Linq.Expressions;
using System.Reflection.PortableExecutable;
using System.Data;
using System.IO;
using OfficeOpenXml;

namespace plcdemo
{
    class Program
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(Program));

        [DllImport("plcommpro.dll", EntryPoint = "Connect")]
        static extern IntPtr Connect(string Parameters);

        [DllImport("plcommpro.dll", EntryPoint = "PullLastError")]
        static extern int PullLastError();

        [DllImport("plcommpro.dll", EntryPoint = "SetDeviceData")]
        public static extern int SetDeviceData(IntPtr h, string tablename, string data, string options);

        private static List<string[]> LoadDataFromFile(string filePath)
        {
            List<string[]> dataRows = new List<string[]>();

            // Check the file extension to determine the appropriate method for reading the data
            string fileExtension = Path.GetExtension(filePath);

            if (fileExtension == ".xlsx" || fileExtension == ".xls")
            {
                // Read data from an Excel file
                using (var excelPackage = new ExcelPackage(new FileInfo(filePath)))
                {
                    var worksheet = excelPackage.Workbook.Worksheets[0]; // Assuming data is in the first worksheet

                    int rows = worksheet.Dimension.Rows;
                    int columns = worksheet.Dimension.Columns;

                    for (int row = 1; row <= rows; row++)
                    {
                        string[] rowData = new string[columns];

                        for (int col = 1; col <= columns; col++)
                        {
                            rowData[col - 1] = worksheet.Cells[row, col].Value?.ToString() ?? "";
                        }

                        dataRows.Add(rowData);
                    }
                }
            }
            else if (fileExtension == ".csv")
            {
                // Read data from a CSV file
                using (var reader = new StreamReader(filePath))
                {
                    while (!reader.EndOfStream)
                    {
                        string line = reader.ReadLine();
                        string[] rowData = line.Split(',');

                        dataRows.Add(rowData);
                    }
                }
            }
            else
            {
                Console.WriteLine("Unsupported file format.");
            }

            return dataRows;
        }

        static void Main(string[] args)
        {
            try
            {
                IntPtr h = IntPtr.Zero;
                int ret = 0;
                string timestamp = "";

                string logConfigFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log4net.config");
                XmlConfigurator.Configure(new FileInfo(logConfigFile));
                log4net.Config.XmlConfigurator.ConfigureAndWatch(new FileInfo(logConfigFile));
                log4net.Config.BasicConfigurator.Configure();
                log4net.Util.LogLog.InternalDebugging = true;

                Console.WriteLine(logConfigFile + "logfile");

                string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
                string configFilePath = Path.Combine(baseDirectory, "app.config");
                Console.WriteLine("Configuration file path: " + configFilePath);

                string connectionString = ConfigurationManager.ConnectionStrings["MyConnectionString"].ConnectionString;

                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    string deviceQueryString = "SELECT * FROM device WHERE enable = 1";
                    using (MySqlCommand deviceCommand = new MySqlCommand(deviceQueryString, connection))
                    {
                        connection.Open();
                        using (MySqlDataReader deviceReader = deviceCommand.ExecuteReader())
                        {
                            List<string> nameList = new List<string>();
                            List<string> ipAddressList = new List<string>();
                            List<int> portList = new List<int>();
                            List<int> timeoutList = new List<int>();
                            List<string> passwordList = new List<string>();

                            while (deviceReader.Read())
                            {
                                string name = deviceReader.GetString("name");
                                string ipAddress = deviceReader.GetString("ipAddress");
                                //int port = deviceReader.GetInt32("port");
                                int port = deviceReader.IsDBNull(deviceReader.GetOrdinal("port")) ? 0 : deviceReader.GetInt32("port");

                                int timeout = deviceReader.GetInt32("timeout");
                                string password = deviceReader.GetString("password");

                                nameList.Add(name);
                                ipAddressList.Add(ipAddress);
                                portList.Add(port);
                                timeoutList.Add(timeout);
                                passwordList.Add(password);
                            }

                            deviceReader.Close();

                            for (int i = 0; i < ipAddressList.Count; i++)
                            {
                                string name = nameList[i];
                                string ipAddress = ipAddressList[i];
                                int port = portList[i];
                                int timeout = timeoutList[i];
                                string password = passwordList[i];

                                string parameters = $"protocol=TCP,ipaddress={ipAddress},port={port},timeout={timeout},passwd={password}";

                                logger.Info($"Connecting to device: {name}, IP: {ipAddress}, Port: {port}, Timeout: {timeout}");

                                h = Connect(parameters);

                                logger.Info($"Connection handle: {h}");

                                string json = "";
                                String[] cards = null;
                                if (h != IntPtr.Zero)
                                {
                                    logger.Info($"Connect device succeed! Device: {name}");

                                    string devtablename = "user";

                                    // Specify the path to your Excel or CSV file
                                   // string filePath = "C:\\Users\\Admin\\Desktop\\srinivas\\ConsoleApp6\\emp.xlsx";
                                    string filePath = ConfigurationManager.AppSettings["ExcelFilePath"];
                                    List<string[]> dataRows = LoadDataFromFile(filePath);

                                    if (dataRows == null || dataRows.Count == 0)
                                    {
                                        Console.WriteLine("No data found in the file.");
                                        continue;
                                    }

                                    StringBuilder dataBuilder = new StringBuilder();

                                    foreach (string[] rowData in dataRows)
                                    {
                                        dataBuilder.Append(string.Join("\t", rowData)).Append("\r\n");
                                    }

                                    string data = dataBuilder.ToString();
                                    Console.WriteLine("data" + data);
                                    string options = "";

                                    ret = SetDeviceData(h, devtablename, data, options);

                                    if (ret >= 0)
                                    {
                                        Console.WriteLine("SetDeviceData operation succeeded!");
                                    }
                                    else
                                    {
                                        Console.WriteLine("SetDeviceData operation failed!");
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("Connect device failed!");
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Error: {ex.Message}");
                logger.Error($"StackTrace: {ex.StackTrace}");
            }

        }
    }
}
