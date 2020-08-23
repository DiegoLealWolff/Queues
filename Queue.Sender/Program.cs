using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Queue.Sender
{  
    public class Program
    {
        const string QueueConnectionString = "Endpoint=sb://geekburguerdiegowolffservicebus.servicebus.windows.net/;SharedAccessKeyName=ProductPolicy;SharedAccessKey=hyPvBASPC5yAtXSI/UFMNLDWETBJD46jXFrXbhNU880=";
        const string QueuePath = "productchanged";
        static IQueueClient queueClient;

        public static async Task Main(string[] args)
        {
            if (args.Length <= 0 || args[0] == "sender")
            {
                await SendMessagesAsync();

                Console.WriteLine("messages were sent");

                Console.ReadKey();

                await queueClient.CloseAsync();
            }            
            else
                Console.WriteLine("nothing to do");
        }

        private static async Task SendMessagesAsync()
        {
            queueClient = new QueueClient(QueueConnectionString, QueuePath);

            queueClient.OperationTimeout = TimeSpan.FromSeconds(10);

            var messages = " Hi,Hello,Hey,How are you,Be Welcome"
                .Split(',')
                .Select(msg =>
                {
                    Console.WriteLine($"Will send message: {msg}");

                    return new Message(Encoding.UTF8.GetBytes(msg));
                })
                .ToList();

            var sendTask = queueClient.SendAsync(messages);

            await sendTask;

            CheckCommunicationExceptions(sendTask);          
        }

        public static bool CheckCommunicationExceptions(Task task)
        {
            if (task.Exception == null || task.Exception.InnerExceptions.Count == 0)
                return true;

            task.Exception.InnerExceptions.ToList()
                .ForEach(innerException =>
                {
                    Console.WriteLine($"Error in SendAsync task: { innerException.Message}.Details: { innerException.StackTrace}");

                    if (innerException is ServiceBusCommunicationException)
                        Console.WriteLine("Connection Problem with Host");
                });

            return false;
        }
    }
}
