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
using System.Xml;

namespace ClassLibrary
{
    public class ClassLibrary
    {
        public DeviceClient client;
        public OpcClient OPC;

        public ClassLibrary(DeviceClient deviceClient, OpcClient OPC)
        {
            this.client = deviceClient;
            this.OPC = OPC;
        }

        #region BrowseNodes
        public async Task Browse(OpcNodeInfo node, List<String> Devices, int level = 0)
        {
            ;
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

        #region PrintData
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
            Console.WriteLine("-----------------------");
        }
        #endregion

        #region D2C send messages
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
                await SendMessagesToIoTHub(newData);
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
                await SendMessagesToIoTHub(newData);
            }

        }


        public async Task SendMessagesToIoTHub(dynamic Data)
        {
            var dataString = JsonConvert.SerializeObject(Data);                     // Musimy zmienic nasza strukturę na String za pomocą JsonConvertera

            Message eventMessage = new Message(Encoding.UTF8.GetBytes(dataString)); // Tworzymy obiekt Wiadomość jako bajty (UTF8)

            // Ustawiamy Headery
            eventMessage.ContentType = MediaTypeNames.Application.Json;             // Typ contentu to jest Json
            eventMessage.ContentEncoding = "utf-8";                                 // Jakie kodowanie

            await client.SendEventAsync(eventMessage);                              // Wysyłamy wiadomość
        }
        #endregion D2C send messages



        #region UpdateTwinAsync
        public async Task UpdateTwinAsync(string DeviceName, object DeviceError)
        {
            var twin = await client.GetTwinAsync();                     // pobieramy twin
            var reportedProperties = twin.Properties.Reported;          // ścieżka dla reported properties

            string nameDevice = DeviceName.Replace(" ", "");              // Z nazwy Device usuwamy spacje
            var errorDevice = DeviceError;


            // jeśli w reported properties jest już ten device (nazwa device)
            if (reportedProperties.Contains(nameDevice))
            {
                var reportedError = reportedProperties[nameDevice];     // pobierz wartosc deviceError z reported properties

                // Jeśli wartość deviceError w reported properties jest różna od wartości błędu w danych(data)
                // wykonaj aktualizację
                if (reportedError != errorDevice)
                {
                    var updatedProperties = new TwinCollection();
                    updatedProperties[nameDevice + "_errors"] = errorDevice;        // Aktualizuj reported property dla device

                    await client.UpdateReportedPropertiesAsync(updatedProperties);
                    Console.WriteLine($"Reported property for deivce |{nameDevice}| was updated!");
                }
                // Jeśli wartość deviceError w reported properties jest taka sama jak wartość błędu w danych(data)
                // to nic nie zmieniamy
                else
                {
                    Console.WriteLine($"No update performed for device |{nameDevice}|!");
                }
            }
            // jesli w reported properties nie ma device (nazwy device)
            else
            {
                // Dodajemy wartość errorDevice dla device w properties
                var updatedProperties = new TwinCollection();
                updatedProperties[nameDevice + "_errors"] = errorDevice;
                await client.UpdateReportedPropertiesAsync(updatedProperties);
                Console.WriteLine($"Added device |{nameDevice}| to reported properties!");

            }
        }
        #endregion

        #region deleteTwinAsync
        public async Task deleteTwinAsync(List<String> deviceList)
        {
            var twin = await client.GetTwinAsync();                     // pobieramy twin
            var reportedProperties = twin.Properties.Reported;          // ścieżka dla reported properties

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
                        await client.UpdateReportedPropertiesAsync(updatedProperties);
                    }
                }

            }
        }
        #endregion

        #region UpdateProductionRate
        #endregion


        public async Task InitializeHandlers()
        {

        }


    }
}