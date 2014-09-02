using System;
using System.Collections.Generic;
using Bootstrap.Extensions.StartupTasks;
using Manufacturing.DataCollector;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Manufacturing.Framework.Utility;

namespace Manufacturing.DataPusher
{
    /// <summary>
    /// This is a basic implementation of a service to pull from the local queue, and push
    /// those messages to a remote queue.
    /// </summary>
    /// <remarks>
    /// This is a terrible implementation that needs to be dramatically improved.
    /// </remarks>
    public class Pusher : IStartupTask
    {
        private readonly ITimer _timer;
        private readonly IDateTime _dateTime;
        private readonly DataPusherConfiguration _config;
        private readonly ILocalRecordRepository _repo;
        private readonly IDataPusher _remotePusher;
        
        private DateTime _lastPush;

        public Pusher(ITimer timer, IDateTime dateTime, DataPusherConfiguration configuration, ILocalRecordRepository repository, IDataPusher remotePusher)
        {
            _timer = timer;
            _dateTime = dateTime;
            _config = configuration;
            _repo = repository;
            _remotePusher = remotePusher;

            _timer.Tick += TimerOnTick;
        }

        private void TimerOnTick(object sender, TimerEventArgs timerEventArgs)
        {
            PushNow();
        }

        private void PushNow()
        {
            while (true)
            {
                _lastPush = _dateTime.UtcNow;

                _repo.ProcessRecords(ProcessRecords, _config.PushBatchSize);

                var nextPushTime = _lastPush.AddSeconds(_config.PushIntervalSeconds);
                var now = _dateTime.UtcNow;
                if (nextPushTime <= now)
                    continue;
                
                _timer.Change(nextPushTime.Subtract(now), TimeSpan.Zero);
                break;
            }
        }

        private void ProcessRecords(IEnumerable<DatasourceRecord> records)
        {
            _remotePusher.PushRecords(records);
        }

        public void Run()
        {
            PushNow();
        }

        public void Reset()
        {
        }
    }
}
