using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using log4net;
using Manufacturing.Framework.DataPusher;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Microsoft.ServiceBus.Messaging;
using Murmur;

namespace Manufacturing.DataPusher
{
    public class ServiceBusQueueDataPusher : IDataPusher
    {
        private readonly DataPusherConfiguration _config;
        private readonly IDatasourceRecordSerializer _recordSerializer;

        private static MessagingFactory _factory;

        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public ServiceBusQueueDataPusher(DataPusherConfiguration configuration, IDatasourceRecordSerializer recordSerializer)
        {
            _config = configuration;
            _recordSerializer = recordSerializer;
            _factory = MessagingFactory.CreateFromConnectionString(_config.ReceiverQueue.GetConnectionString());
        }

        public void PushRecords(IEnumerable<DatasourceRecord> records)
        {
            //TODO: determine how the queues will be named
            var sender = _factory.CreateMessageSender(_config.ReceiverQueue.QueueName);

            var recList = records.ToList(); //avoid reenumerations
            using (var ms = _recordSerializer.Serialize(recList))
            {
                string hashString;

                using (var hasher = MurmurHash.Create128())
                {
                    var hash = hasher.ComputeHash(ms);
                    hashString = Encoding.UTF8.GetString(hash);
                }
                ms.Position = 0;

                var msg = new BrokeredMessage(ms, false) { MessageId = hashString };

                var sw = new Stopwatch();
                sw.Start();
                sender.Send(msg);
                sw.Stop();

                Log.DebugFormat("Pushed {0} records to topic/queue in {1}ms", recList.Count, sw.ElapsedMilliseconds);
            }
        }
    }
}