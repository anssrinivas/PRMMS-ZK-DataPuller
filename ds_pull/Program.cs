using log4net;
using log4net.Config;
using log4net.Repository.Hierarchy;
using MySql.Data.MySqlClient;
using Org.BouncyCastle.Ocsp;
using System.Configuration;
using System.Data;
using System.Runtime.InteropServices;
using System.Text;
class Program
{
    private static readonly ILog logger = LogManager.GetLogger(typeof(Program));

    [DllImport("plcommpro.dll", EntryPoint = "Connect")]
    static extern IntPtr Connect(string Parameters);

    [DllImport("plcommpro.dll", EntryPoint = "PullLastError")]
    static extern int PullLastError();

    [DllImport("plcommpro.dll", EntryPoint = "GetDeviceData")]
    public static extern int GetDeviceData(IntPtr h, ref byte buffer, int buffersize, string tablename, string filename, string filter, string options);


    [DllImport("plcommpro.dll", EntryPoint = "DeleteDeviceData")]
    public static extern int DeleteDeviceData(IntPtr h, string tablename, string data, string options);
    [DllImport("plcommpro.dll", EntryPoint = "GetDeviceDataCount")]
    public static extern int GetDeviceDataCount(IntPtr h, string tablename, string filter, string options);
    static void Main()
    {
        try
        {
            IntPtr connectionHandle = IntPtr.Zero;
            int dataRetrievalResult = 0;
            int Devicecount = 0;
            string timestamp = "";
            int logsRead = 0;
            // Configure log4net using a configuration file
            string logConfigFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log4net.config");
            XmlConfigurator.Configure(new FileInfo(logConfigFile));
            log4net.Config.XmlConfigurator.ConfigureAndWatch(new FileInfo(logConfigFile));
            log4net.Config.BasicConfigurator.Configure();
            log4net.Util.LogLog.InternalDebugging = true;

            Console.WriteLine(logConfigFile + "logfile");

            // Retrieve the connection string from the configuration file
            string connectionString = ConfigurationManager.ConnectionStrings["MyConnectionString"].ConnectionString;

            // Connect to the MySQL database
            // ...
            // Connect to the MySQL database
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                // Query to retrieve enabled devices from the "device" table
                string deviceQueryString = "SELECT * FROM device WHERE enable = 1";

                // Execute the device query
                using (MySqlCommand deviceCommand = new MySqlCommand(deviceQueryString, connection))
                {
                    connection.Open();

                    // Read the devices returned by the query
                    using (MySqlDataReader deviceReader = deviceCommand.ExecuteReader())
                    {
                        List<string> nameList = new List<string>();
                        List<string> ipAddressList = new List<string>();
                        List<int> portList = new List<int>();
                        List<int> timeoutList = new List<int>();
                        List<string> passwordList = new List<string>();

                        // Fetch device details and store them in separate lists
                        while (deviceReader.Read())
                        {
                            try
                            {
                                string deviceName = deviceReader.GetString("name");
                                string deviceIpAddress = deviceReader.GetString("ipAddress");
                                int devicePort = deviceReader.GetInt32("port");
                                int deviceTimeout = deviceReader.GetInt32("timeout");
                                string devicePassword = deviceReader.GetString("password");

                                nameList.Add(deviceName);
                                ipAddressList.Add(deviceIpAddress);
                                portList.Add(devicePort);
                                timeoutList.Add(deviceTimeout);
                                passwordList.Add(devicePassword);
                            }
                            catch (Exception ex)
                            {
                                logger.Error($"Error while fetching device details: {ex.Message}");
                            }
                        }

                        deviceReader.Close();

                        // Process each device separately
                        for (int deviceIndex = 0; deviceIndex < nameList.Count; deviceIndex++)
                        {
                            try
                            {
                                string deviceName = nameList[deviceIndex];
                                string deviceIpAddress = ipAddressList[deviceIndex];
                                int devicePort = portList[deviceIndex];
                                int deviceTimeout = timeoutList[deviceIndex];
                                string devicePassword = passwordList[deviceIndex];

                                // Build the connection parameters string
                                string parameters = $"protocol=TCP,ipaddress={deviceIpAddress},port={devicePort},timeout={deviceTimeout},passwd={devicePassword}";

                                logger.Info($"Connecting to device: {deviceName}, IP: {deviceIpAddress}, Port: {devicePort}, Timeout: {deviceTimeout}");

                                // Connect to the device using the external DLL function
                                connectionHandle = Connect(parameters);

                                logger.Info($"Connection handle: {connectionHandle}");

                                string json = "";
                                string[] cards = null;

                                // If the connection is successful
                                if (connectionHandle != IntPtr.Zero)
                                {
                                    logger.Info($"Connect device succeed! Device: {deviceName}");
                                    int bufferSize = int.Parse(ConfigurationManager.AppSettings["BufferSize"]);

                                    byte[] buffer = new byte[bufferSize];
                                    string dataFilter = "";
                                    string options = "";
                                    string dataString = "";
                                    string[] tmp = null;
                                    string dataTableName = "transaction";
                                    string fileName = "";
                                    string filter = "";

                                    // Retrieve device data using the external DLL function
                                    dataRetrievalResult = GetDeviceData(connectionHandle, ref buffer[0], bufferSize, dataTableName, dataString, dataFilter, options);

                                    logger.Info($"Connection handle: {connectionHandle}, Data retrieval result: {dataRetrievalResult}");

                                    dataString = Encoding.Default.GetString(buffer);
                                    string[] dataEntries = dataString.Split("\n");
                                    

                                    // Process each data entry received from the device
                                    for (int entryIndex = 0; entryIndex < dataEntries.Length; entryIndex++)
                                    {
                                        try
                                        {
                                            logsRead++;
                                            string entry = dataEntries[entryIndex];
                                            logger.Info($"Data received: {entry}");

                                            cards = entry.Split(",");

                                            // Parse the timestamp from the received data
                                            if (int.TryParse(cards[6], out int timeInSeconds))
                                            {
                                                int seconds = timeInSeconds % 60;
                                                int minutes = (timeInSeconds / 60) % 60;
                                                int hours = (timeInSeconds / 3600) % 24;
                                                int day = (timeInSeconds / 86400) % 31 + 1;
                                                int month = (timeInSeconds / 2678400) % 12 + 1;
                                                int year = (timeInSeconds / 32140800) + 2000;

                                                timestamp = $"{year}-{month:00}-{day:00} {hours:00}:{minutes:00}:{seconds:00}";
                                            }
                                            else
                                            {
                                                logger.Warn("Timestamp is not in the correct format: " + timestamp);
                                            }
                                            
                                            // Retrieve the module and hostname from the configuration
                                            string module = ConfigurationManager.AppSettings["Module"];
                                            string hostname = ConfigurationManager.AppSettings["Hostname"];
                                            string dataType = ConfigurationManager.AppSettings["dataType"];
                                            
                                            {
                                                // Build the JSON data using the received card data and other details
                                                json = "{\"Cardno\":" + cards[0] + ",\"Pin\":" + cards[1] + ",\"Verified\":" + cards[2] + ",\"DoorID\":" + cards[3] + ",\"EventType\":" + cards[4] + ",\"InOutState\":" + cards[5] + ",\"Time_second\":\"" + timestamp + "\",\"IPAddress\":\"" + deviceIpAddress + "\",\"hostname\":\"" + hostname + "\"}";

                                                logger.Info($"JSON data: {json}");

                                                // Check if the connection is closed and open it if necessary
                                                if (connection.State != ConnectionState.Open)
                                                {
                                                    try
                                                    {
                                                        connection.Open();
                                                        // Write audit log for successful database connection opening
                                                        logger.Info("Database connection opened successfully");
                                                    }
                                                    catch (MySqlException ex)
                                                    {
                                                        // Write error log for failed database connection opening
                                                        logger.Info("Failed to open database connection: " + ex.Message);
                                                    }
                                                }

                                                // Insert the JSON data into the database
                                                string sql = $"INSERT INTO ds_publish_outbox(data, module, data_type) VALUES (@json, @module, @data_type)";

                                                MySqlCommand command = new MySqlCommand(sql, connection);
                                                command.Parameters.AddWithValue("@json", json);
                                                command.Parameters.AddWithValue("@module", module);
                                                command.Parameters.AddWithValue("@data_type", dataType);

                                                int rowsAffected = command.ExecuteNonQuery();
                                                logger.Info($"Rows affected: {rowsAffected}");

                                                Devicecount = GetDeviceDataCount(connectionHandle, dataTableName, dataFilter, options);

                                                logger.Info($"Device count Total: {Devicecount}");
                                                if (rowsAffected > 0 || Devicecount == dataRetrievalResult)
                                                {
                                                    int dataDeleteResult = DeleteDeviceData(connectionHandle, dataTableName, dataString, options);
                                                    // Check if the deletion operation was successful
                                                    logger.Info($"Number of Rows are deleted: {rowsAffected}");
                                                }
                                                else
                                                {
                                                    logger.Info("No rows deleted.");
                                                }
 }
                                        }
                                        catch (Exception e)
                                        {
                                            logger.Error("Error while inserting in table: " + e.Message);
                                        }
                                    }
                                }
                                else
                                {
                                    logger.Error($"Failed to connect to device: {deviceName}, IP: {deviceIpAddress}, Error: {PullLastError()}");
                                }
                            }
                            catch (Exception ex)
                            {
                                logger.Error($"Error in device loop: {ex.Message}");
                            }
                        }
                    }
                }
                // ...
            }
        }
        catch (Exception ex)
        {
            logger.Error($"Error: {ex.Message}");
        }
    }
}