using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using QueueWorkerService.Context;
using QueueWorkerService.Models;
using QueueWorkerService.Services;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace QueueWorkerService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private IModel _channel;
        private RabbitMQClientService _rabbitMQClientService;
        public static string QueueName = "product.new";

        private readonly IServiceProvider _serviceProvider;

        public Worker(RabbitMQClientService rabbitMQClientService, ILogger<Worker> logger, IServiceProvider serviceProvider)
        {
            _rabbitMQClientService = rabbitMQClientService;
            _logger = logger;
            _serviceProvider = serviceProvider;
        }
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _channel = _rabbitMQClientService.Connect();

            _channel.BasicQos(prefetchSize: 0,
                              prefetchCount: 1,
                              global: false);

            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new AsyncEventingBasicConsumer(_channel);

            _channel.BasicConsume(queue: QueueName,
                                  autoAck: false,
                                  consumer: consumer);

            consumer.Received += Consumer_Received;

            await Task.Delay(1000, stoppingToken);
        }

        private Task Consumer_Received(object sender, BasicDeliverEventArgs @event)
        {
            try
            {

                var productMessage = JsonSerializer.Deserialize<Product>(Encoding.UTF8.GetString(@event.Body.ToArray()));

                using (var scope = _serviceProvider.CreateScope())
                {
                    var context = scope.ServiceProvider.GetRequiredService<DataContext>();

                    context.Add(productMessage);

                    context.SaveChanges();

                    //context.Dispose();

                    //scope.Dispose();
                }

                //_context.Products.AddAsync(productMessage);

                _channel.BasicAck(@event.DeliveryTag, false);

                _logger.LogInformation("Task Completed.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
            }

            return Task.CompletedTask;
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            return base.StopAsync(cancellationToken);
        }
    }
}
