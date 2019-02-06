using CsvHelper;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace SendMessagesTopic
{
    public class ServiceBusManager
    {
        private string serviceBusConnectionString;
        private string topicName;
        private static TopicClient topicClient;

        public ServiceBusManager()
        {
            serviceBusConnectionString = "write the service bus connection string";
        }

        public IEnumerable<BlobFile> ReadCsvFile(string filePath)
        {
            List<BlobFile> listOperationId = new List<BlobFile>();

            if (File.Exists(filePath))
            {
                using (var textReader = File.OpenText(filePath))
                {
                    using (var csv = new CsvReader(textReader))
                    {
                        listOperationId = csv.GetRecords<BlobFile>().ToList();
                    }
                }
            }
            else
            {
                throw new Exception($"El archivo {filePath} no existe, por favor verifique la ruta de su archivo.");
            }

            return listOperationId;
        }

        internal string GetMessageFromFile(string validFolerPath, string id)
        {
            var message = File.ReadAllText($"{validFolerPath}\\{id}");
            return message;
        }

        internal bool SearchFile(string id, string validFolerPath)
        {
            return File.Exists($"{validFolerPath}\\{id}");
        }

        public async Task SendMessageToTopic(Dictionary<string, string> messages, string topicName)
        {
            this.topicName = topicName;

            topicClient = TopicClient.CreateFromConnectionString(serviceBusConnectionString, topicName);

            foreach (var message in messages)
            {
                Console.WriteLine($"Enviando el mensaje {message.Key}");
                var brokerMessage = new BrokeredMessage(message.Value);
                await topicClient.SendAsync(brokerMessage);
                Console.WriteLine($"Mensaje {message.Key} enviado!");
            }

            await topicClient.CloseAsync();
        }
    }
}