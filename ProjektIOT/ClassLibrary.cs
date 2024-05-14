using Opc.UaFx;
using Opc.UaFx.Client;
using System.Net.Mime;
using System.Text;
using Newtonsoft.Json;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Shared;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Devices.Client.Exceptions;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Win32;
using System.Net.Sockets;
using System.Xml.Linq;
using Azure.Messaging.ServiceBus;
using Opc.Ua;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.Devices;
using System.Configuration;
using System.Collections.Specialized;


namespace ClassLibrary
{
    public class ClassLibrary
    {
        public DeviceClient client;
        public OpcClient OPC;
        public RegistryManager registry;


        #region Constructor
        public ClassLibrary(DeviceClient deviceClient, OpcClient OPC, RegistryManager registry)
        {
            this.client = deviceClient;
            this.OPC = OPC;
            this.registry = registry;
        }
        #endregion

        #region BrowseNodes -> Make a list of devices in simulation
        public async Task Browse(OpcNodeInfo node, List<String> Devices, int level = 0)
        {
            //;
            level++;

            foreach (var childNode in node.Children())
            {
                if (childNode.DisplayName.Value.Contains("Device "))
                {
                    Devices.Add(childNode.DisplayName.Value);
                }
                Browse(childNode, Devices, level);
            }

        }
        #endregion

        #region PrintData -> Print readed from nodes parameters
        public async Task PrintData(string DeviceName,
                                    object ProductionStatus,
                                    object ProductionRate,
                                    object WorkorderId,
                                    object Temperature,
                                    object GoodCount,
                                    object BadCount,
                                    object DeviceError)
        {
            Console.WriteLine(DeviceName);
            Console.WriteLine("ProductionStatus: " + ProductionStatus);
            Console.WriteLine("ProductionRate:   " + ProductionRate);
            Console.WriteLine("WorkorderId:      " + WorkorderId);
            Console.WriteLine("Temperature:      " + Temperature);
            Console.WriteLine("GoodCount:        " + GoodCount);
            Console.WriteLine("BadCount:         " + BadCount);
            Console.WriteLine("DeviceError:      " + DeviceError);
            Console.WriteLine("------------------------------------------------------");
        }
        #endregion

        #region D2C SendMessages -> Send data with or without DeviceError value (depends on change on DeviceErrors in simulation
        public async Task SendMessages(string DeviceName,
                                        object ProductionStatus,
                                        object ProductionRate,
                                        object WorkorderId,
                                        object Temperature,
                                        object GoodCount,
                                        object BadCount,
                                        object DeviceError)
        {
            var twin = await client.GetTwinAsync();
            var reportedProperties = twin.Properties.Reported;
            var nameDevice = DeviceName.Replace(" ", "")+"_errors";
            var errorDevice = DeviceError;
            bool sameData = true;

            if (reportedProperties.Contains(nameDevice))
            {
                var currentError = reportedProperties[nameDevice];

                sameData = (currentError == errorDevice);
            }
            if (sameData)
            {
                var newData = new
                {
                    DeviceName = DeviceName,
                    ProductionStatus = ProductionStatus,
                    WorkorderId = WorkorderId,
                    GoodCount = GoodCount,
                    BadCount = BadCount,
                    Temperature = Temperature,
                    ProductionRate = ProductionRate
                };
                await SendMessagesToIoTHub(newData, sameData);
            }
            else
            {
                var newData = new
                {
                    DeviceName = DeviceName,
                    ProductionStatus = ProductionStatus,
                    WorkorderId = WorkorderId,
                    GoodCount = GoodCount,
                    BadCount = BadCount,
                    Temperature = Temperature,
                    ProductionRate = ProductionRate,
                    DeviceError = DeviceError
                };
                await SendMessagesToIoTHub(newData, sameData);
            }
        }

        public async Task SendMessagesToIoTHub(dynamic Data, bool sameData)
        {
            var dataString = JsonConvert.SerializeObject(Data);                     // Musimy zmienic nasza strukturę na String za pomocą JsonConvertera

            Microsoft.Azure.Devices.Client.Message eventMessage = new Microsoft.Azure.Devices.Client.Message(Encoding.UTF8.GetBytes(dataString)); // Tworzymy obiekt Wiadomość jako bajty (UTF8)
            eventMessage.Properties.Add("errorAlert", (!sameData ) ? "true" : "false");

            // Ustawiamy Headery
            eventMessage.ContentType = MediaTypeNames.Application.Json;             // Typ contentu to jest Json
            eventMessage.ContentEncoding = "utf-8";                                 // Jakie kodowanie

            await client.SendEventAsync(eventMessage);                              // Wysyłamy wiadomość
        }
        #endregion 

        #region UpdateTwinAsync - change of Device_errors and Device_productionRate in Desired and Reported properties in DeviceTwin
        public async Task UpdateTwinAsync(string DeviceName, object DeviceError, object ProductionRate)
        {
            var twin = await client.GetTwinAsync();                     // pobieramy twin
          
            var reportedProperties = twin.Properties.Reported;          // pobieramy reported properties
            var desiredProperties = twin.Properties.Desired;            // pobieramy deired properties


            // DEVICE_ERRORS -----------------------------------------------
            // DecviceName_errors dla reported properties
            string deviceName_errors = DeviceName.Replace(" ", "") + "_errors";  
            var errorDevice = DeviceError;
            // jeśli w reported properties jest już ten device_error
            if (reportedProperties.Contains(deviceName_errors))
            {
                var reportedError = reportedProperties[deviceName_errors];     // pobierz wartosc device_error z reported properties

                // Jeśli wartość device_error w reported properties jest różna od wartości błędu w danych(data)
                // wykonaj aktualizację
                if (reportedError != errorDevice)
                {
                    var updatedProperties = new TwinCollection();
                    updatedProperties[deviceName_errors] = errorDevice;        // Aktualizuj reported property dla device

                    await client.UpdateReportedPropertiesAsync(updatedProperties);
                    Console.WriteLine($"Reported property for {deviceName_errors} was updated!");
                }
                // Jeśli wartość device_error w reported properties jest taka sama jak wartość błędu w danych(data)
                // to nic nie zmieniamy
                else
                {
                    Console.WriteLine($"No update performed for {deviceName_errors}!");
                }
            }
            // jesli w reported properties nie ma device_error
            else
            {
                // Dodajemy wartość device_error dla device w properties
                var updatedProperties = new TwinCollection();
                updatedProperties[deviceName_errors] = errorDevice;
                await client.UpdateReportedPropertiesAsync(updatedProperties);
                Console.WriteLine($"Added device {deviceName_errors} to reported properties!");

            }

            // PRODUCTION_RATE ---------------------------------------------------------
            // DeviceName_production_rate dla desired properties
            var deviceName_production_rate = DeviceName.Replace(" ", "") + "_production_rate";
            var productionRate = ProductionRate;

            if (desiredProperties.Contains(deviceName_production_rate))
            {
                int ProductionRate_numeric;
                if (int.TryParse((string)desiredProperties[deviceName_production_rate], out ProductionRate_numeric)) 
                {
                    OpcStatus tmp = OPC.WriteNode("ns=2;s=" + DeviceName + "/ProductionRate", ProductionRate_numeric);
                }
            }

            if (reportedProperties.Contains(deviceName_production_rate))
            {
                var reportedProductionRate = reportedProperties[deviceName_production_rate];
                // Jeśli aktualna wartosc productonRate jest rozpozna od nowej,
                // wykonaj aktualizacje
                if (reportedProductionRate != ProductionRate) 
                {
                    var updatedProperties = new TwinCollection();
                    updatedProperties[deviceName_production_rate] = ProductionRate;

                    await client.UpdateReportedPropertiesAsync(updatedProperties);
                    Console.WriteLine($"Reported property for {deviceName_production_rate} was updated!");
                }
                else
                {
                    Console.WriteLine($"No update performed for {deviceName_production_rate}!");
                }
            }
            else
            // jesli w reported properties nie ma production_rate
            {
                // Dodajemy wartość production_rate dla device w properties
                var updatedProperties = new TwinCollection();
                updatedProperties[deviceName_production_rate] = ProductionRate;
                await client.UpdateReportedPropertiesAsync(updatedProperties);
                Console.WriteLine($"Added {deviceName_production_rate} to reported properties!");

            }
        }
        #endregion

        #region DeleteTwinAsync -> Delete unneeded reported/desired properties for Device_errors and Device_production_rate
        public async Task deleteTwinAsync(List<String> deviceList)
        {
            var twin = await client.GetTwinAsync();                     // pobieramy twin dla reported properties
            var desired_twin = await registry.GetTwinAsync(ConfigurationManager.AppSettings.Get("IOTHubName"));
            var reportedProperties = twin.Properties.Reported;          // ścieżka dla reported properties
            var desiredProperties = desired_twin.Properties.Desired;

            foreach (var property in reportedProperties)
            {
                if (property.ToString().Substring(1, 6) == "Device")
                {
                    string reportedDevice = property.ToString().Substring(1).Split("_")[0];                          // nazwa device z reported properties, np. Device2
                    if (!deviceList.Contains(reportedDevice.Substring(1, 6) + " " + reportedDevice.Substring(7)))   // sprawdz czy Lista device'ow zawiera reportedDevice już ze spacją
                    {
                        // jesli nie zawiera to usuwamy ten device z reported properties
                        var deleteDevice = reportedDevice;
                        var updatedProperties = new TwinCollection();
                        updatedProperties[deleteDevice + "_errors"] = null;
                        updatedProperties[deleteDevice + "_production_rate"] = null;
                        await client.UpdateReportedPropertiesAsync(updatedProperties);
                    }
                }
            }
            // deleting desired properties
            /*
            foreach (var property in desiredProperties)
            {
                if (property.ToString().Substring(1, 6) == "Device")
                {
                    string reportedDevice = property.ToString().Substring(1).Split("_")[0];                          // nazwa device z reported properties, np. Device2
                    if (!deviceList.Contains(reportedDevice.Substring(1, 6) + " " + reportedDevice.Substring(7)))   // sprawdz czy Lista device'ow zawiera reportedDevice już ze spacją
                    {
                        // jesli nie zawiera to usuwamy ten device z desired properties
                        var deleteDevice = reportedDevice;
                        //var updatedProperties = new TwinCollection();
                        //desiredProperties[deleteDevice + "_production_rate"] = null;
                        //await registry.UpdateTwinAsync(desired_twin.DeviceId, desired_twin, desired_twin.ETag);
                    }
                }
            } */
        }
        #endregion 

        #region EmergencyStop -> direct method
        public async Task<MethodResponse> EmergencyStop(MethodRequest methodRequest, object userContext)
        {
            var payload = JsonConvert.DeserializeAnonymousType(methodRequest.DataAsJson, new { deviceName = default(string) });
            if (payload != null)
            {
                Console.WriteLine("Emergency stop for " + payload.deviceName);

                await Task.Run(() => OPC.CallMethod("ns=2;s=" + payload.deviceName, "ns=2;s=" + payload.deviceName + "/EmergencyStop"));
            }
            else
            {
                Console.WriteLine("Emergency stop for " + payload.deviceName + "done!");
            }
            return new MethodResponse(0);

        }
        #endregion

        #region ResetErrorStatus -> direct method
        public async Task<MethodResponse> ResetErrorStatus(MethodRequest methodRequest, object userContext)
        {
            var payload = JsonConvert.DeserializeAnonymousType(methodRequest.DataAsJson, new { deviceName = default(string) });
            if (payload != null)
            {
                Console.WriteLine("Reset error status for " + payload.deviceName);

                await Task.Run(() => OPC.CallMethod("ns=2;s=" + payload.deviceName, "ns=2;s=" + payload.deviceName + "/ResetErrorStatus"));
            }
            else
            {
                Console.WriteLine("Reset error status for " + payload.deviceName + "done!");
            }
            return new MethodResponse(0);
        }
        #endregion

        #region Handlers
        public async Task InitializeHandlers()
        {
            await client.SetMethodHandlerAsync("EmergencyStop", EmergencyStop, client);
            await client.SetMethodHandlerAsync("ResetErrorStatus", ResetErrorStatus, client);

        }
        #endregion

        #region Business logic -> serviceBus for more then 3 device errors in 1 minute = emergencyStop
        public async Task ProcessMessageAsync_for_errors(ProcessMessageEventArgs arg)
        {
            //Console.WriteLine($"RECEIVED MESSAGE FOR ERRORS:\n\t{arg.Message.Body}");
            var message = Encoding.UTF8.GetString(arg.Message.Body);
            ReadMessage_for_errors mesg = JsonConvert.DeserializeObject<ReadMessage_for_errors>(message);

            string deviceId = mesg.DeviceName;
            OPC.CallMethod($"ns=2;s={deviceId}", $"ns=2;s={deviceId}/EmergencyStop");
        }

        public Task ProcessErrorAsync_for_errors(ProcessErrorEventArgs arg)
        {
            Console.WriteLine(arg.Exception.ToString());

            return Task.CompletedTask;
        }
        public class ReadMessage_for_errors
        {
            public string DeviceName { get; set; }
            public string Count { get; set; }

        }
        #endregion

        #region Business logic -> serviceBus for kpi below 90 in 5 minutes = decrease productionRate
        public async Task DecreaseProductionRate(string IOThub, string deviceName)
        {
            var twin = await registry.GetTwinAsync(IOThub);
            string json = JsonConvert.SerializeObject(twin, Formatting.Indented);
            JObject data = JObject.Parse(json);

            var reportedProperties = twin.Properties.Reported;
            var desiredProperties = twin.Properties.Desired;       

            var productionRate_name = deviceName.Replace(" ", "") + "_production_rate";


            if (desiredProperties.Contains(productionRate_name))
            {
             
                int desired_value = desiredProperties[productionRate_name];
                int decreasedRate = desired_value - 10;

                
                desiredProperties[productionRate_name] = decreasedRate;
                await registry.UpdateTwinAsync(twin.DeviceId, twin, twin.ETag);                                                           
                OpcStatus status = OPC.WriteNode($"ns=2;s={deviceName}/ProductionRate", decreasedRate);

            }
            else
            {
                int new_productionRate = reportedProperties[productionRate_name];
                int decreasedRate = new_productionRate - 10;

                desiredProperties[productionRate_name] = decreasedRate;
                await registry.UpdateTwinAsync(twin.DeviceId, twin, twin.ETag);
                Console.WriteLine($"Added {productionRate_name} to desired properties!");
                OpcStatus status = OPC.WriteNode($"ns=2;s={deviceName}/ProductionRate", decreasedRate);

            }
        }

        
        public async Task ProcessMessageAsync_for_kpi(ProcessMessageEventArgs arg)
        {
            //Console.WriteLine($"RECEIVED MESSAGE FOR KPI:\n\t{arg.Message.Body}");
            var message = Encoding.UTF8.GetString(arg.Message.Body);
            ReadMessage_for_errors mesg = JsonConvert.DeserializeObject<ReadMessage_for_errors>(message);

            string deviceName = mesg.DeviceName;
            string IOThub = ConfigurationManager.AppSettings.Get("IOTHubName");
            await DecreaseProductionRate(IOThub, deviceName);
        }

        public Task ProcessErrorAsync_for_kpi(ProcessErrorEventArgs arg)
        {
            Console.WriteLine(arg.Exception.ToString());

            return Task.CompletedTask;
        }
        public class ReadMessage_for_kpi
        {
            public string DeviceName { get; set; }
            public string KPI { get; set; }

        }
        #endregion
    }
}