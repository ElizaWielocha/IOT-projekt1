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

            foreach (var childNode in node.Children())                  // iterate trough devices in simulation
            {
                if (childNode.DisplayName.Value.Contains("Device "))
                {
                    Devices.Add(childNode.DisplayName.Value);           // add device to list
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

        #region D2C SendMessages -> Send data with or without DeviceError value (depends on change on DeviceErrors in simulation)
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
            var nameDeviceErrors = DeviceName.Replace(" ", "")+"_errors";
            var errorDevice = DeviceError;
            bool sameData = true;

            if (reportedProperties.Contains(nameDeviceErrors))
            {
                var currentError = reportedProperties[nameDeviceErrors];

                sameData = (currentError == errorDevice);       // check if device_error value changed
            }
            if (sameData)                                       // device_error value didnt change
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
            else                                                // device_error changed
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
            var dataString = JsonConvert.SerializeObject(Data);                     // change data structure to string

            Microsoft.Azure.Devices.Client.Message eventMessage = new Microsoft.Azure.Devices.Client.Message(Encoding.UTF8.GetBytes(dataString)); // creating object message as bites (UTF8)
            eventMessage.Properties.Add("errorAlert", (!sameData && Data.DeviceError != 0) ? "true" : "false"); // alert event : true if device_error value changed 

             
            eventMessage.ContentType = MediaTypeNames.Application.Json;             // Content type is json 
            eventMessage.ContentEncoding = "utf-8";                                 // utf coding

            await client.SendEventAsync(eventMessage);                              // sending message
        }
        #endregion 

        #region UpdateTwinAsync - change of Device_errors and Device_productionRate in Desired and Reported properties in DeviceTwin
        public async Task UpdateTwinAsync(string DeviceName, object DeviceError, object ProductionRate)
        {
            var twin = await client.GetTwinAsync();                     // get the twin
          
            var reportedProperties = twin.Properties.Reported;          // get the reported properties
            var desiredProperties = twin.Properties.Desired;            // get the deired properties


            // DEVICE_ERRORS -----------------------------------------------
            // DecviceName_errors for reported properties
            string deviceName_errors = DeviceName.Replace(" ", "") + "_errors";  
            var errorDevice = DeviceError;
            // if this device_error is in reported properties 
            if (reportedProperties.Contains(deviceName_errors))
            {
                var reportedError = reportedProperties[deviceName_errors];     // get the value of device_error from reported properties

                // if device_error value in reported properties is different then data.DeviceError
                // do update 
                if (reportedError != errorDevice)
                {
                    var updatedProperties = new TwinCollection();
                    updatedProperties[deviceName_errors] = errorDevice;        // Update reported property 

                    await client.UpdateReportedPropertiesAsync(updatedProperties);
                    Console.WriteLine($"Reported property for {deviceName_errors} was updated!");
                }
                // if device_error value in reported properties is the same as data.DeviceError
                // then nothing
                else
                {
                    Console.WriteLine($"No update performed for {deviceName_errors}!");
                }
            }
            // if this device_error is NOT in reported properties
            else
            {
                // add device_error value for the device in reported properties
                var updatedProperties = new TwinCollection();
                updatedProperties[deviceName_errors] = errorDevice;
                await client.UpdateReportedPropertiesAsync(updatedProperties);
                Console.WriteLine($"Added device {deviceName_errors} to reported properties!");

            }

            // PRODUCTION_RATE ---------------------------------------------------------
            // DeviceName_production_rate for desired properties
            var deviceName_production_rate = DeviceName.Replace(" ", "") + "_production_rate";
            var productionRate = ProductionRate;
            // if this device_productionRate is in desired properties 
            if (desiredProperties.Contains(deviceName_production_rate))
            {
                // change the simulation productionRate to desired productionRate
                int ProductionRate_numeric;
                if (int.TryParse((string)desiredProperties[deviceName_production_rate], out ProductionRate_numeric)) 
                {
                    OpcStatus tmp = OPC.WriteNode("ns=2;s=" + DeviceName + "/ProductionRate", ProductionRate_numeric);
                }
            }
            // if this device_productionRate is in reported properties
            if (reportedProperties.Contains(deviceName_production_rate))
            {
                var reportedProductionRate = reportedProperties[deviceName_production_rate];
                // if current productonRate is different then the reported productionRate
                // do update reported properties
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
            // if this device_productionRate is NOT in reported properties
            {
                // add production_rate value to this device in reported properties
                var updatedProperties = new TwinCollection();
                updatedProperties[deviceName_production_rate] = ProductionRate;
                await client.UpdateReportedPropertiesAsync(updatedProperties);
                Console.WriteLine($"Added {deviceName_production_rate} to reported properties!");

            }
        }
        #endregion

        #region DeleteTwinAsync -> Delete unneeded reported properties for Device_errors and Device_production_rate
        public async Task deleteTwinAsync(List<String> deviceList)
        {
            var twin = await client.GetTwinAsync();                     // get twin for reported properties
            var desired_twin = await registry.GetTwinAsync(ConfigurationManager.AppSettings.Get("IOTHubName"));
            var reportedProperties = twin.Properties.Reported;          // get the reported properties
            var desiredProperties = desired_twin.Properties.Desired;

            foreach (var property in reportedProperties)
            {
                if (property.ToString().Substring(1, 6) == "Device")
                {
                    string reportedDevice = property.ToString().Substring(1).Split("_")[0];                         // device name from reported properties, eg. Device2
                    if (!deviceList.Contains(reportedDevice.Substring(1, 6) + " " + reportedDevice.Substring(7))) 
                    {
                        // if device is not in devices list then delete it from reported properties
                        var deleteDevice = reportedDevice;
                        var updatedProperties = new TwinCollection();
                        updatedProperties[deleteDevice + "_errors"] = null;
                        updatedProperties[deleteDevice + "_production_rate"] = null;
                        await client.UpdateReportedPropertiesAsync(updatedProperties);
                    }
                }
            }
        }
        #endregion 

        #region EmergencyStop -> direct method
        // run in IOT EXPLORER, eg: {"DeviceName" : "Device 2"}
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
        // run in IOT EXPLORER, eg: {"DeviceName" : "Device 2"}
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
            // wait for running direct methods
            await client.SetMethodHandlerAsync("EmergencyStop", EmergencyStop, client);
            await client.SetMethodHandlerAsync("ResetErrorStatus", ResetErrorStatus, client);

        }
        #endregion

        #region Business logic -> serviceBus for more then 3 device errors in 1 minute = emergencyStop
        public async Task ProcessMessageAsync_for_errors(ProcessMessageEventArgs arg)
        {
            var message = Encoding.UTF8.GetString(arg.Message.Body);
            ReadMessage_for_errors mesg = JsonConvert.DeserializeObject<ReadMessage_for_errors>(message);

            // after receiving message - do emergency stop
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

            // productionRate is in desired Properties
            if (desiredProperties.Contains(productionRate_name))
            {
             
                int desired_value = desiredProperties[productionRate_name];
                int decreasedRate = desired_value - 10;

                // update desired properties with decreased productionRate
                desiredProperties[productionRate_name] = decreasedRate;
                await registry.UpdateTwinAsync(twin.DeviceId, twin, twin.ETag);                                                           
                OpcStatus status = OPC.WriteNode($"ns=2;s={deviceName}/ProductionRate", decreasedRate);

            }
            // productionRate is NOT in desired Properties
            else
            {
                int new_productionRate = reportedProperties[productionRate_name];
                int decreasedRate = new_productionRate - 10;

                // add decreased productionRate from reported properties to desired properties
                desiredProperties[productionRate_name] = decreasedRate;
                await registry.UpdateTwinAsync(twin.DeviceId, twin, twin.ETag);
                Console.WriteLine($"Added {productionRate_name} to desired properties!");
                OpcStatus status = OPC.WriteNode($"ns=2;s={deviceName}/ProductionRate", decreasedRate);

            }
        }

        
        public async Task ProcessMessageAsync_for_kpi(ProcessMessageEventArgs arg)
        {
            var message = Encoding.UTF8.GetString(arg.Message.Body);
            ReadMessage_for_errors mesg = JsonConvert.DeserializeObject<ReadMessage_for_errors>(message);

            // run decreasing productionRate
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