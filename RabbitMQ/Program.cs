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

            while (true)
            {
                Console.Clear();
                Console.WriteLine("Lista operacji : ");
                Console.WriteLine("1 - Opublikuj wiadomość w kolejce queue1");
                Console.WriteLine("2 - Odczytaj wiadomość z kolejki queue1");
                Console.WriteLine("Podaj cyfrę i potwierdź enterem:");
                string option = Console.ReadLine();
                switch (option)
                {
                    case "1":
                        Publish();
                        break;
                    case "2":
                        break;
                    case "3":
                        break;
                    case "4":
                        break;
                    default:
                        break;
                }
            }

            
            //SubscribeToQueue();
            while (true)
            {
                Receiver();
                
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

        public void SendMessageTopic()
        {
            using (var conn = connFactory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                var properties = channel.CreateBasicProperties();
                properties.Persistent = false;
                byte[] messagebuffer = Encoding.Default.GetBytes("Message from Topic Exchange 'Bombay' ");//to change
                channel.BasicPublish("TopicExchangeTest", "Message.Bombay.Email", properties, messagebuffer);//to change
                Console.WriteLine("Message Sent From :- topic.exchange ");//to change
                Console.WriteLine("Routing Key :- Message.Bombay.Email");//to change
                Console.WriteLine("Message Sent");//to change
            }
        }
        public static void CreateTestExchanges()
        {
            using (var conn = connFactory.CreateConnection() )
            using (var channel = conn.CreateModel())
            {
                channel.ExchangeDeclare("DirectExchangeTest", ExchangeType.Direct);
                channel.QueueDeclare("QueueForDirect_01");
                channel.QueueDeclare("QueueForDirect_02");
                channel.QueueDeclare("QueueForDirect_03");
                channel.QueueBind("QueueForDirect_01", "DirectExchangeTest","RoutingKeyQueue1");
                channel.QueueBind("QueueForDirect_02", "DirectExchangeTest", "RoutingKeyQueue2");
                channel.QueueBind("QueueForDirect_03", "DirectExchangeTest", "RoutingKeyQueue2");

                channel.ExchangeDeclare("FanoutExchangeTest", ExchangeType.Fanout);
                channel.QueueDeclare("QueueForFanout_01");
                channel.QueueDeclare("QueueForFanout_02");
                channel.QueueDeclare("QueueForFanout_03");
                channel.QueueBind("QueueForFanout_01", "FanoutExchangeTest", "1");
                channel.QueueBind("QueueForFanout_02", "FanoutExchangeTest", "2");
                channel.QueueBind("QueueForFanout_03", "FanoutExchangeTest", "3");

                channel.ExchangeDeclare("TopicExchangeTest", ExchangeType.Topic);
                channel.QueueDeclare("Topic.QueueForTopic_01.1");
                channel.QueueDeclare("Topic.QueueForTopic_02.2");
                channel.QueueDeclare("Topic.QueueForTopic_03");
                channel.QueueBind("Topic.*.1", "TopicExchangeTest", "1");
                channel.QueueBind("Topic.*.2", "TopicExchangeTest", "2");
                channel.QueueBind("Topic.#", "TopicExchangeTest", "3");

                channel.ExchangeDeclare("HeadersExchangeTest", ExchangeType.Headers);
                channel.QueueDeclare("QueueForHeaders_01");
                channel.QueueDeclare("QueueForHeaders_02");
                channel.QueueDeclare("QueueForHeaders_03");
                channel.QueueBind("QueueForHeaders_01", "HeadersExchangeTest", "1");
                channel.QueueBind("QueueForHeaders_02", "HeadersExchangeTest", "2");
                channel.QueueBind("QueueForHeaders_03", "HeadersExchangeTest", "3");
            }
        }
    }


}
