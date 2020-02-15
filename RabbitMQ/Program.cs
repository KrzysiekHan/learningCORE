using System;
using System.Configuration;
using System.Text;
using System.Threading;
using RabbitMQ.Client;

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
            Publish();
            Console.WriteLine(GetMessage());
            Console.ReadLine();
        }

        public static void Publish()
        {
            using (var conn = connFactory.CreateConnection())
            {
                using (var channel = conn.CreateModel())
                {
                    var message = DateTime.Now.ToLongDateString();
                    var data = Encoding.UTF8.GetBytes(message);
                    var queueName = "queue1";
                    bool durable = true;
                    bool exclusive = false;
                    bool autoDelete = false;
                    channel.QueueDeclare(queueName, durable, exclusive, autoDelete, null);
                    var exchangeName = "";
                    var routingKey = "queue1";
                    channel.BasicPublish(exchangeName, routingKey, null, data);
                }
            }
        }

        public static string GetMessage()
        {
            using (var conn = connFactory.CreateConnection())
            {
                using (var channel = conn.CreateModel())
                {
                    channel.QueueDeclare("queue1", false, false, false, null);
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
        }
    }
}
