using Manufacturing.Framework.Configuration;

namespace Manufacturing.DataPusher
{
    public class DataPusherConfiguration
    {
        public int PushIntervalSeconds { get; set; }
        public int PushBatchSize { get; set; }
        public ServiceBusQueueInformation ReceiverQueue { get; set; }

        public string EventHubConnectionString { get; set; }
        public string EventHubPath { get; set; }
    }
}
