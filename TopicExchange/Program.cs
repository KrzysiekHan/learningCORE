using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ;
using RabbitMQ.Client;

namespace TopicExchange
{
    class Program
    {
        static void Main(string[] args)
        {
            Topicmessages tm = new Topicmessages();
            tm.SendMessage();
            Console.ReadLine();
        }
    }

    public class Topicmessages
    {
        // CloudAMQP URL in format amqp://user:pass@hostName:port/vhost
        private const string url = "amqps://pxnpxzlt:F2VzPK13wRKw20APTOy3Dk359yTJQ8QD@hawk.rmq.cloudamqp.com/pxnpxzlt";
        private readonly ConnectionFactory connFactory = new ConnectionFactory()
        {
            Uri = new Uri(url)
        };

        public void SendMessage()
        {
            using (var conn = connFactory.CreateConnection())
            using (var model = conn.CreateModel())
            {
                var properties = model.CreateBasicProperties();
                properties.Persistent = false;
                byte[] messagebuffer = Encoding.UTF8.GetBytes("Message from topic exchange Bombay");
                byte[] wrongmessagebuffer = Encoding.UTF8.GetBytes("Incorrect message");
                model.BasicPublish("topic.exchange", "Message.Bombay.Email", properties, messagebuffer);//correct message
                model.BasicPublish("topic.exchange", "Message.Bombay.Wrong.RoutingKey", properties, wrongmessagebuffer);//wrong topic message should be lost
                Console.WriteLine("Message sent from topic.exchange with routing key Message.Bombay.Email...");
            }
        }

    }
}
