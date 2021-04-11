using Amazon.Extensions.NETCore.Setup;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AWSSQS
{
    public class Product
    {
        public string ProductID { get; set; }
        public string ProductName { get; set; }
    }
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    // Class to create a queue
    class Program
    {
        private const string MaxReceiveCount = "10";
        private const string ReceiveMessageWaitTime = "2";
        private const int MaxArgs = 3;

        static async Task Main(string[] args)
        {
            IConfiguration Configuration = new ConfigurationBuilder()
                                            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                                            .AddEnvironmentVariables()
                                            .AddCommandLine(args)
                                            .AddUserSecrets("e140e2ee-9908-44d3-a7a3-f5d3ff47104d")
                                            .Build();
            var section = Configuration.GetSection("AwsSqsConfiguration");
            AWSOptions awsOptions = new AWSOptions
            {
                Credentials = new Amazon.Runtime.BasicAWSCredentials(section.GetSection("AWSAccessKey").Value, section.GetSection("AWSSecretKey").Value),
                Region = Amazon.RegionEndpoint.GetBySystemName(section.GetSection("AWSRegion").Value)
            };
            // Parse the command line and show help if necessary
            var parsedArgs = CommandLine.Parse(args);
            if (parsedArgs.Count > MaxArgs)
            {
                PrintHelp();
                return;
            }
            //CommandLine.ErrorExit(
            //  "\nToo many command-line arguments.\nRun the command with no arguments to see help.");

            // Create the Amazon SQS client
            var sqsClient = new AmazonSQSClient(awsOptions.Credentials, awsOptions.Region);

            // In the case of no command-line arguments, just show help and the existing queues
            if (parsedArgs.Count == 0)
            {
                PrintHelp();
                Console.WriteLine("\nNo arguments specified.");
                Console.Write("Do you want to see a list of the existing queues? ((y) or n): ");
                string response = Console.ReadLine();
                if ((string.IsNullOrEmpty(response)) || (response.ToLower() == "y"))
                    await ShowQueues(sqsClient);
                return;
            }

            // Get the application parameters from the parsed arguments
            string queueName =
              CommandLine.GetParameter(parsedArgs, null, "-q", "--queue-name");
            string deadLetterQueueUrl =
              CommandLine.GetParameter(parsedArgs, null, "-d", "--dead-letter-queue");
            string maxReceiveCount =
              CommandLine.GetParameter(parsedArgs, MaxReceiveCount, "-m", "--max-receive-count");
            string receiveWaitTime =
              CommandLine.GetParameter(parsedArgs, ReceiveMessageWaitTime, "-w", "--wait-time");

            if (string.IsNullOrEmpty(queueName))
                CommandLine.ErrorExit(
                  "\nYou must supply a queue name.\nRun the command with no arguments to see help.");

            // If a dead-letter queue wasn't given, create one
            if (string.IsNullOrEmpty(deadLetterQueueUrl))
            {
                Console.WriteLine("\nNo dead-letter queue was specified. Creating one...");
                deadLetterQueueUrl = await CreateQueue(sqsClient, queueName + "__dlq");
                Console.WriteLine($"Your new dead-letter queue:");
                await ShowAllAttributes(sqsClient, deadLetterQueueUrl);
            }

            // Create the message queue
            string messageQueueUrl = await CreateQueue(
              sqsClient, queueName, deadLetterQueueUrl, maxReceiveCount, receiveWaitTime);
            Console.WriteLine($"Your new message queue:");
            await ShowAllAttributes(sqsClient, messageQueueUrl);
            //Send messages to the queue
            var product1 = new Product { ProductID = "P01", ProductName = "Talcum Powder" };
            await PostMessageAsync<Product>(sqsClient, messageQueueUrl, product1);
            var product2 = new Product { ProductID = "P02", ProductName = "Body Perfume" };
            await PostMessageAsync<Product>(sqsClient, messageQueueUrl, product2);
            //Get messages from queue
            var recdMessage = await GetMessageAsync(sqsClient, messageQueueUrl, 2);
            while (recdMessage.Count > 0)
            {
                foreach (var msg in recdMessage)
                {
                    var obj = JsonConvert.DeserializeObject<Product>(msg.Body);
                    Console.WriteLine("The product received is :");
                    Console.WriteLine($"ID : {obj.ProductID} Name : {obj.ProductName}");
                    //Delete the message from queue
                    await DeleteMessageAsync(sqsClient, messageQueueUrl, msg.ReceiptHandle);
                }
                recdMessage = await GetMessageAsync(sqsClient, messageQueueUrl, 2);
            }
        }


        //
        // Method to show a list of the existing queues
        private static async Task ShowQueues(IAmazonSQS sqsClient)
        {
            ListQueuesResponse responseList = await sqsClient.ListQueuesAsync("");
            Console.WriteLine();
            foreach (string qUrl in responseList.QueueUrls)
            {
                // Get and show all attributes. Could also get a subset.
                await ShowAllAttributes(sqsClient, qUrl);
            }
        }

        //
        //Method to get messages from queue
        private static async Task<List<Message>> GetMessageAsync(IAmazonSQS sqsClient, string qUrl, int waitTime)
        {
            var response = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = qUrl,
                WaitTimeSeconds = waitTime,
                AttributeNames = new List<string> { "ApproximateReceiveCount" },
                MessageAttributeNames = new List<string> { "All" }
            });
            return response.Messages;
        }

        //
        //Method to send message to queue
        public static async Task PostMessageAsync<T>(IAmazonSQS sqsClient, string qUrl, T message)
        {
            var attrs = new Dictionary<string, MessageAttributeValue>();
            
            var sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = qUrl,
                MessageBody = JsonConvert.SerializeObject(message),
                MessageAttributes = attrs
            };
            //sendMessageRequest.MessageGroupId = typeof(T).Name;
            //sendMessageRequest.MessageDeduplicationId = Guid.NewGuid().ToString();
            await sqsClient.SendMessageAsync(sendMessageRequest);
        }

        //
        // Method to create a queue. Returns the queue URL.
        private static async Task<string> CreateQueue(
        IAmazonSQS sqsClient, string qName, string deadLetterQueueUrl = null,
        string maxReceiveCount = null, string receiveWaitTime = null)
        {
            var attrs = new Dictionary<string, string>();

            // If a dead-letter queue is given, create a message queue
            if (!string.IsNullOrEmpty(deadLetterQueueUrl))
            {
                attrs.Add(QueueAttributeName.ReceiveMessageWaitTimeSeconds, receiveWaitTime);
                attrs.Add(QueueAttributeName.RedrivePolicy,
                  $"{{\"deadLetterTargetArn\":\"{await GetQueueArn(sqsClient, deadLetterQueueUrl)}\"," +
                  $"\"maxReceiveCount\":\"{maxReceiveCount}\"}}");
                // Add other attributes for the message queue such as VisibilityTimeout
            }

            // If no dead-letter queue is given, create one of those instead
            //else
            //{
            //  // Add attributes for the dead-letter queue as needed
            //  attrs.Add();
            //}

            // Create the queue
            CreateQueueResponse responseCreate = await sqsClient.CreateQueueAsync(
                new CreateQueueRequest { QueueName = qName, Attributes = attrs });
            return responseCreate.QueueUrl;
        }

        //
        //Method to delete message from a queue
        public static async Task DeleteMessageAsync(IAmazonSQS sqsClient, string qUrl, string receiptHandle)
        {
            await sqsClient.DeleteMessageAsync(new DeleteMessageRequest
            {
                QueueUrl = qUrl,
                ReceiptHandle = receiptHandle
            });
        }

        //
        // Method to get the ARN of a queue
        private static async Task<string> GetQueueArn(IAmazonSQS sqsClient, string qUrl)
        {
            GetQueueAttributesResponse responseGetAtt = await sqsClient.GetQueueAttributesAsync(
              qUrl, new List<string> { QueueAttributeName.QueueArn });
            return responseGetAtt.QueueARN;
        }


        //
        // Method to show all attributes of a queue
        private static async Task ShowAllAttributes(IAmazonSQS sqsClient, string qUrl)
        {
            var attributes = new List<string> { QueueAttributeName.All };
            GetQueueAttributesResponse responseGetAtt =
              await sqsClient.GetQueueAttributesAsync(qUrl, attributes);
            Console.WriteLine($"Queue: {qUrl}");
            foreach (var att in responseGetAtt.Attributes)
                Console.WriteLine($"\t{att.Key}: {att.Value}");
        }


        //
        // Command-line help
        private static void PrintHelp()
        {
            Console.WriteLine(
            "\nUsage: SQSCreateQueue -q <queue-name> [-d <dead-letter-queue>]" +
              " [-m <max-receive-count>] [-w <wait-time>]" +
            "\n  -q, --queue-name: The name of the queue you want to create." +
            "\n  -d, --dead-letter-queue: The URL of an existing queue to be used as the dead-letter queue." +
            "\n      If this argument isn't supplied, a new dead-letter queue will be created." +
            "\n  -m, --max-receive-count: The value for maxReceiveCount in the RedrivePolicy of the queue." +
            $"\n      Default is {MaxReceiveCount}." +
            "\n  -w, --wait-time: The value for ReceiveMessageWaitTimeSeconds of the queue for long polling." +
            $"\n      Default is {ReceiveMessageWaitTime}.");
        }
    }


    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    // Class that represents a command line on the console or terminal.
    // (This is the same for all examples. When you have seen it once, you can ignore it.)
    public static class CommandLine
    {
        // Method to parse a command line of the form: "--param value" or "-p value".
        // If "param" is found without a matching "value", Dictionary.Value is an empty string.
        // If "value" is found without a matching "param", Dictionary.Key is "--NoKeyN"
        //  where "N" represents sequential numbers.
        public static Dictionary<string, string> Parse(string[] args)
        {
            var parsedArgs = new Dictionary<string, string>();
            int i = 0, n = 0;
            while (i < args.Length)
            {
                // If the first argument in this iteration starts with a dash it's an option.
                if (args[i].StartsWith("-"))
                {
                    var key = args[i++];
                    var value = string.Empty;

                    // Is there a value that goes with this option?
                    if ((i < args.Length) && (!args[i].StartsWith("-"))) value = args[i++];
                    parsedArgs.Add(key, value);
                }

                // If the first argument in this iteration doesn't start with a dash, it's a value
                else
                {
                    parsedArgs.Add("--NoKey" + n.ToString(), args[i++]);
                    n++;
                }
            }

            return parsedArgs;
        }

        //
        // Method to get a parameter from the parsed command-line arguments
        public static string GetParameter(
          Dictionary<string, string> parsedArgs, string def, params string[] keys)
        {
            string retval = null;
            foreach (var key in keys)
                if (parsedArgs.TryGetValue(key, out retval)) break;
            return retval ?? def;
        }

        //
        // Exit with an error.
        public static void ErrorExit(string msg, int code = 1)
        {
            Console.WriteLine("\nError");
            Console.WriteLine(msg);
            Environment.Exit(code);
        }
    }
}
