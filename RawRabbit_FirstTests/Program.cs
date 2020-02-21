
using RawRabbit;
using RawRabbit.vNext;
using Serilog;
using System;
using System.Threading.Tasks;

namespace RawRabbit_FirstTests
{
    class Program
    {
		private static IBusClient _client;

		public static void Main(string[] args)
		{
			RunAsync().GetAwaiter().GetResult();
		}

		public static async Task RunAsync()
		{
			Log.Logger = new LoggerConfiguration()
				.WriteTo.LiterateConsole()
				.CreateLogger();

			_client = RawRabbitFactory.CreateSingleton(new RawRabbitOptions
			{
				ClientConfiguration = new ConfigurationBuilder()
					.SetBasePath(Directory.GetCurrentDirectory())
					.AddJsonFile("rawrabbit.json")
					.Build()
					.Get<RawRabbitConfiguration>(),
				Plugins = p => p
					.UseGlobalExecutionId()
					.UseMessageContext<MessageContext>()
			});

			await _client.SubscribeAsync<ValuesRequested, MessageContext>((requested, ctx) => ServerValuesAsync(requested, ctx));
			await _client.RespondAsync<ValueRequest, ValueResponse>(request => SendValuesThoughRpcAsync(request));
		}

		private static Task<ValueResponse> SendValuesThoughRpcAsync(ValueRequest request)
		{
			return Task.FromResult(new ValueResponse
			{
				Value = $"value{request.Value}"
			});
		}

		private static Task ServerValuesAsync(ValuesRequested message, MessageContext ctx)
		{
			var values = new List<string>();
			for (var i = 0; i < message.NumberOfValues; i++)
			{
				values.Add($"value{i}");
			}
			return _client.PublishAsync(new ValuesCalculated { Values = values });
		}
	}
}
