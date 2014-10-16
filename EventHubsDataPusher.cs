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

        public async void PushRecords(IEnumerable<DatasourceRecord> records)
        {
            long bytes = 0;

            var recs = records.ToArray();
            var messages = new List<EventData>();
            
            foreach (var t in recs)
            {
                var serialized = _recordSerializer.Serialize(new[] {t});
                bytes += serialized.Length;
                var ed = new EventData(serialized);

                //Later, if we want to batch, we could use "DatasourceRecordBatch"
                ed.Properties.Add("Type", "DatasourceRecord");
                ed.Properties.Add("Serializer", _recordSerializer.GetType().Name);

                messages.Add(ed);
            }

            if (messages.Count <= 0) return;

            var sw = new Stopwatch();
            sw.Start();

            //Should I implement a batch size back off?
            var client = _eventHubClients[0];
            await client.SendBatchAsync(messages);

            sw.Stop();

            var msgsPerSec = (1000.0/sw.ElapsedMilliseconds)*recs.Length;
            Log.DebugFormat("Pushed {0} records to Event Hubs in {1}ms ({2:0} msgs / sec, {3:0} total KB)",
                recs.Length, sw.ElapsedMilliseconds, msgsPerSec, bytes/1024.0);
        }
    }
}