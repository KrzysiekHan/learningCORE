using System;
using System.Configuration;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ
{
    class Program
    {
        // CloudAMQP URL in format amqp://user:pass@hostName:port/vhost
        static string url = "amqps://pxnpxzlt:F2VzPK13wRKw20APTOy3Dk359yTJQ8QD@hawk.rmq.cloudamqp.com/pxnpxzlt";
        static readonly ConnectionFactory connFactory = new ConnectionFactory() {
            Uri = new Uri(url)
        };

        static void Main(string[] args)
        {
            //SubscribeToQueue();
            while (true)
            {
                Receiver();
                Publish();
                Thread.Sleep(5000);
            }
        }

        public static void SubscribeToQueue()
        {
            using (var conn = connFactory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                channel.BasicQos(0, 1, false);
                MessageReceiver messageReceiver = new MessageReceiver(channel);
                channel.BasicConsume("queue1", false, messageReceiver);
            }      
        }

        public static void Publish()
        {
            using (var conn = connFactory.CreateConnection())
            {
                using (var channel = conn.CreateModel())
                {
                    var message = DateTime.Now.ToLocalTime().ToString();
                    var data = Encoding.UTF8.GetBytes(message);
                    var queueName = "queue1";
                    bool durable = true;
                    bool exclusive = false;
                    bool autoDelete = false;
                    channel.QueueDeclare(queueName, durable, exclusive, autoDelete, null);
                    var exchangeName = "";
                    var routingKey = "queue1";
                    channel.BasicPublish(exchangeName, routingKey, null, data);
                    Console.WriteLine("Message sent...");
                }
            }
        }

        public static string GetMessage()
        {
            using (var conn = connFactory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                channel.QueueDeclare("queue1", true, false, false, null);
                var queueName = "queue1";
                var data = channel.BasicGet(queueName, false);
                if (data == null)
                {
                    return "no message found";
                }
                var message = Encoding.UTF8.GetString(data.Body);
                channel.BasicAck(data.DeliveryTag, false);
                return message;
            }            
        }

        public static void Receiver()
        {
            Console.WriteLine("odbieranie wiadomości");
            using (var conn = connFactory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                channel.QueueDeclare(
                    queue: "queue1",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($" [x] otrzymano {message}");
                };

                // accept only one unack-ed message at a time
                // uint prefetchSize, ushort prefetchCount, bool global
                channel.BasicQos(0, 1, false);

                channel.BasicConsume(
                    queue: "queue1",
                    autoAck: true,
                    consumer: consumer
                    );
                Console.WriteLine("Wciśnij enter aby wyłączyć aplikację");
                Console.ReadLine();
            }
        }
    }


}
