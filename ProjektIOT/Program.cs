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
using System.Net.Sockets;
using Microsoft.Azure.Devices;
using Azure.Messaging.ServiceBus;
using System.Diagnostics;
using Microsoft.Azure.Devices;

/*
string deviceConnectionString;
string localhostConnection;
// menu 
Console.WriteLine("Do you want to use the default connection? [y/n]: ");
char choice = Console.ReadLine()[0];
if (choice.Equals('y'))
{
    deviceConnectionString = $"HostName=Uczelnia-Zajecia.azure-devices.net;DeviceId=Device_test;SharedAccessKey=NpE3SJIrdSphmNeEZyU5ZNIGh6hG0tn3oAIoTGnPtco=";
    localhostConnection = "opc.tcp://localhost:4840/";
}
else
{
    deviceConnectionString = Console.ReadLine();
    localhostConnection = Console.ReadLine();
}
*/

var deviceConnectionString = $"HostName=Uczelnia-Zajecia.azure-devices.net;DeviceId=Device_test;SharedAccessKey=NpE3SJIrdSphmNeEZyU5ZNIGh6hG0tn3oAIoTGnPtco=";
using var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, Microsoft.Azure.Devices.Client.TransportType.Mqtt);   // uzycie klasy 
await deviceClient.OpenAsync();                                                                                 // otwarcie połączenia z IOT HUBem

const string sbConnectionString = "Endpoint=sb://servicebusproject.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=UvfL+IFASe542tBJI3lNRFw0jJRe/qUnl+ASbPabuYE=";



using (var client = new OpcClient("opc.tcp://localhost:4840/"))
{
    client.Connect();
    using var registry = RegistryManager.CreateFromConnectionString("HostName=Uczelnia-Zajecia.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=gEkhKZ4eq2jMwDecCXFbtAqfjh49wQRhDAIoTFmWzsQ=");
    var device = new ClassLibrary.ClassLibrary(deviceClient, client, registry);
    await device.InitializeHandlers();

    var node = client.BrowseNode(OpcObjectTypes.ObjectsFolder);
    List<string> devicesList = new List<string>();
    await device.Browse(node, devicesList);

    await device.deleteTwinAsync(devicesList);
    Console.WriteLine("Client connected!");


    // servisbus
    await using ServiceBusClient sbClient = new ServiceBusClient(sbConnectionString);

    await using ServiceBusProcessor processor_for_errors = sbClient.CreateProcessor("errors-queue");
    processor_for_errors.ProcessMessageAsync += device.ProcessMessageAsync_for_errors;
    processor_for_errors.ProcessErrorAsync += device.ProcessErrorAsync_for_errors;

    await using ServiceBusProcessor processor_for_kpi = sbClient.CreateProcessor("kpi-queue");
    processor_for_kpi.ProcessMessageAsync += device.ProcessMessageAsync_for_kpi;
    processor_for_kpi.ProcessErrorAsync += device.ProcessErrorAsync_for_kpi;
    //

    while (true)
    {
        Console.Clear();

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
            Console.WriteLine("------------------------------------------------------");
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

            await device.UpdateTwinAsync(data.DeviceName, data.DeviceError, data.ProductionRate);

            // servisbus
            await processor_for_errors.StartProcessingAsync();
            Thread.Sleep(300);
            await processor_for_errors.StopProcessingAsync();
            await processor_for_kpi.StartProcessingAsync();
            Thread.Sleep(300);
            await processor_for_kpi.StopProcessingAsync();
            //
            Console.WriteLine("");
        }
        Thread.Sleep(3000);
    }
}