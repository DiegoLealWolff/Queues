using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Queue.Receiver
{
    public class Program
    {
        const string QueueConnectionString = "Endpoint=sb://geekburguerdiegowolffservicebus.servicebus.windows.net/;SharedAccessKeyName=ProductPolicy;SharedAccessKey=hyPvBASPC5yAtXSI/UFMNLDWETBJD46jXFrXbhNU880=";
        const string QueuePath = "productchanged";
        static IQueueClient queueClient;
        public static List<Task> PendingCompleteTasks = new List<Task>();

        public static async Task Main(string[] args)
        {
            await ReceiveMessagesAsync();

            Console.WriteLine("messages were received");

            Console.ReadKey();

            await queueClient.CloseAsync();

            Console.ReadLine();
        }

        private static async Task ReceiveMessagesAsync()
        {
            queueClient = new QueueClient(QueueConnectionString, QueuePath, ReceiveMode.PeekLock);

            queueClient.RegisterMessageHandler(MessageHandler, new MessageHandlerOptions(ExceptionHandler) { AutoComplete = false });

            Console.ReadLine();

            Console.WriteLine($" Request to close async. Pending tasks: { PendingCompleteTasks.Count } ");

            await Task.WhenAll(PendingCompleteTasks);

            Console.WriteLine($"All pending tasks were completed");

            var closeTask = queueClient.CloseAsync();

            await closeTask;

            CheckCommunicationExceptions(closeTask);
        }

        private static Task ExceptionHandler(ExceptionReceivedEventArgs exceptionArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionArgs.Exception}.");

            var context = exceptionArgs.ExceptionReceivedContext;

            Console.WriteLine($"Endpoint:{context.Endpoint}, Path:{context.EntityPath}, Action:{context.Action}");

            return Task.CompletedTask;
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

        private static async Task MessageHandler(Message message, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Received message { Encoding.UTF8.GetString(message.Body)}");

            if (cancellationToken.IsCancellationRequested || queueClient.IsClosedOrClosing)
                return;

            int count = 0;

            Console.WriteLine($"task {count++}");

            Task PendingCompleteTask;

            lock (PendingCompleteTasks)
            {
                PendingCompleteTasks.Add(queueClient.CompleteAsync(message.SystemProperties.LockToken));

                PendingCompleteTask = PendingCompleteTasks.LastOrDefault();
            }

            Console.WriteLine($"calling complete for task {count}");

            await PendingCompleteTask;

            Console.WriteLine($"remove task {count} from task queue");

            PendingCompleteTasks.Remove(PendingCompleteTask);
        }
    }
}
