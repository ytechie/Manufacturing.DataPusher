using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using log4net;
using Manufacturing.Framework.DataPusher;
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

        private static EventHubClient _eventHub;

        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public EventHubsDataPusher(DataPusherConfiguration configuration, IDatasourceRecordSerializer recordSerializer)
        {
            _recordSerializer = recordSerializer;

            var factory = MessagingFactory.Create("sb://jymfx-ns.servicebus.windows.net/", new MessagingFactorySettings()
            {
                TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider("send", "ZfOSLx83PtF2mwgh1DO285XQupK67bNEPg41QgHC7GQ="),
                TransportType = TransportType.Amqp,
                AmqpTransportSettings = new AmqpTransportSettings { BatchFlushInterval = TimeSpan.FromSeconds(5) }
            });
            _eventHub = factory.CreateEventHubClient(configuration.EventHubPath);
            //_eventHub = EventHubClient.CreateFromConnectionString(configuration.EventHubConnectionString, configuration.EventHubPath);

        }

        public void PushRecords(IEnumerable<DatasourceRecord> records)
        {
            var tasks = new List<Task>();
            var count = 0;
            long bytes = 0;

            var recs = records.ToList();

            var sw = new Stopwatch();
            sw.Start();
            foreach (var record in recs)
            {
                var serialized = _recordSerializer.Serialize(new[] {record});
                bytes += serialized.Length;
                var ed = new EventData(serialized);

                //Later, if we want to batch, we could use "DatasourceRecordBatch"
                ed.Properties.Add("Type", "DatasourceRecord");

                tasks.Add(_eventHub.SendAsync(ed));
                count++;
            }
            Task.WaitAll(tasks.ToArray());
            sw.Stop();

            var msgsPerSec = (1000.0/sw.ElapsedMilliseconds)*count;
            Log.DebugFormat("Pushed {0} records to Event Hubs in {1}ms ({2:0} msgs / sec, {3:0} total KB)",
                count, sw.ElapsedMilliseconds, msgsPerSec, bytes / 1024.0);
        }
    }
}