using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace SendMessagesTopic
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            try
            {
                Console.WriteLine("Sending message to the topic");
                string rootFolderPath = args[0];
                string filePath = $"{rootFolderPath}{args[1]}";
                string invalidFolerPath = $"{rootFolderPath}{args[2]}";
                string validFolerPath = $"{rootFolderPath}{args[3]}";
                ServiceBusManager serviceBusManager = new ServiceBusManager();

                var listOperationId = serviceBusManager.ReadCsvFile(filePath);
                var validMessages = new Dictionary<string, string>();
                var invalidMessages = new Dictionary<string, string>();

                foreach (var item in listOperationId)
                {
                    if (serviceBusManager.SearchFile(item.Id, validFolerPath))
                    {
                        var message = serviceBusManager.GetMessageFromFile(validFolerPath, item.Id);
                        validMessages.Add(item.Id, message);
                    }
                    else
                    {
                        if (serviceBusManager.SearchFile(item.Id, invalidFolerPath))
                        {
                            var message = serviceBusManager.GetMessageFromFile(invalidFolerPath, item.Id);
                            invalidMessages.Add(item.Id, message);
                        }
                    }
                }

                serviceBusManager.SendMessageToTopic(validMessages, TopicsEnum.validfiletopic.ToString()).GetAwaiter().GetResult();
                serviceBusManager.SendMessageToTopic(invalidMessages, TopicsEnum.invalidfiletopic.ToString()).GetAwaiter().GetResult();

                Console.WriteLine("Messages sent it!");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            finally
            {
                stopwatch.Stop();
            }
            Console.WriteLine($"Ha finalizado el proceso en: {stopwatch}");
            Console.ReadKey();
        }
    }
}