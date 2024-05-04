using Opc.UaFx;
using Opc.UaFx.Client;
using System.Net.Mime;
using System.Text;
using Newtonsoft.Json;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Shared;
using ClassLibrary;
using System.IO;
using Microsoft.Azure.Amqp.Framing;
using System;
using System.Collections.Generic;
using System.IO;




// Tworzymy klienta i łączymy się z IOT HUB ---------------------------------------------------------

string deviceConnectionString = "HostName=Uczelnia-Zajecia.azure-devices.net;DeviceId=Device_test;SharedAccessKey=NpE3SJIrdSphmNeEZyU5ZNIGh6hG0tn3oAIoTGnPtco=";

    using var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Mqtt);   // uzycie klasy 
    await deviceClient.OpenAsync();                                                                                 // otwarcie połączenia z IOT HUBem
                                                                          // wlasciwa deklaracja klienta



using (var client = new OpcClient("opc.tcp://localhost:4840/"))
{
    client.Connect();
    var device = new Class1(deviceClient, client);
    
    var node = client.BrowseNode(OpcObjectTypes.ObjectsFolder);
    List<string> devicesList = new List<string>();
    await device.Browse(node, devicesList);

    await device.deleteTwinAsync(devicesList);
    Console.WriteLine("Client connected!");
    while (true)
    {
        client.Connect();
        device = new Class1(deviceClient, client);

        Console.Clear();
        Thread.Sleep(1000);
        foreach (string deviceName in devicesList)
        {
            string pre = "ns=2;s=" + deviceName + "/";

            var data = new     // tworzymy strukture danych przechowujaca nasza wiadomosc
            {
                DeviceName = deviceName,
                ProductionStatus = client.ReadNode(pre + "ProductionStatus").Value,
                ProductionRate = client.ReadNode(pre + "ProductionRate").Value,
                WorkorderId = client.ReadNode(pre + "WorkorderId").Value,
                Temperature = client.ReadNode(pre + "Temperature").Value,
                GoodCount = client.ReadNode(pre + "GoodCount").Value,
                BadCount = client.ReadNode(pre + "BadCount").Value,
                DeviceError = client.ReadNode(pre + "DeviceError").Value
            };

            await device.PrintData(data.DeviceName,
                                    data.ProductionStatus,
                                    data.ProductionRate,
                                    data.WorkorderId,
                                    data.Temperature,
                                    data.GoodCount,
                                    data.BadCount,
                                    data.DeviceError);

            await device.SendMessages(data.DeviceName,
                                        data.ProductionStatus,
                                        data.ProductionRate,
                                        data.WorkorderId,
                                        data.Temperature,
                                        data.GoodCount,
                                        data.BadCount,
                                        data.DeviceError);

            await device.UpdateTwinAsync(data.DeviceName, data.DeviceError);
            

        }
        client.Disconnect();
        await Task.Delay(500);
    }
}