using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using log4net;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceBus.Messaging.Amqp;

namespace Manufacturing.DataPusher
{
    public class EventHubsDataPusher : IDataPusher
    {
        private readonly IDatasourceRecordSerializer _recordSerializer;

        private readonly int _processorCount; //cache this to avoid lookups
        private readonly List<EventHubClient> _eventHubClients;

        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public EventHubsDataPusher(DataPusherConfiguration configuration, IDatasourceRecordSerializer recordSerializer)
        {
            _recordSerializer = recordSerializer;

            _processorCount = Environment.ProcessorCount;
            _eventHubClients = new List<EventHubClient>(_processorCount);

            var factory = MessagingFactory.Create(
                "sb://" + configuration.EventHubNamespace + ".servicebus.windows.net/",
                new MessagingFactorySettings
                {
                    TokenProvider =
                        TokenProvider.CreateSharedAccessSignatureTokenProvider(configuration.EventHubSharedAccessKeyName,
                            configuration.EventHubSharedAccessKey),
                    TransportType = TransportType.Amqp,
                    AmqpTransportSettings = new AmqpTransportSettings()
                });

            for (var i = 0; i < _processorCount; i++)
            {
                var newConnnection = factory.CreateEventHubClient(configuration.EventHubPath);
         
                _eventHubClients.Add(newConnnection);
            }

            Log.DebugFormat("Initialized {0} Event Hub clients", _processorCount);
        }

        public void PushRecords(IEnumerable<DatasourceRecord> records)
        {
            var tasks = new List<Task>();
            long bytes = 0;

            var recs = records.ToArray();

            var sw = new Stopwatch();
            sw.Start();
            for(var i = 0; i < recs.Length; i++)
            {
                var serialized = _recordSerializer.Serialize(new[] {recs[i]});
                bytes += serialized.Length;
                var ed = new EventData(serialized);

                //Later, if we want to batch, we could use "DatasourceRecordBatch"
                ed.Properties.Add("Type", "DatasourceRecord");

                var client = _eventHubClients[i%_processorCount];

                tasks.Add(client.SendAsync(ed));
            }
            Task.WaitAll(tasks.ToArray());
            sw.Stop();

            var msgsPerSec = (1000.0/sw.ElapsedMilliseconds)*recs.Length;
            Log.DebugFormat("Pushed {0} records to Event Hubs in {1}ms ({2:0} msgs / sec, {3:0} total KB)",
                recs.Length, sw.ElapsedMilliseconds, msgsPerSec, bytes / 1024.0);
        }
    }
}