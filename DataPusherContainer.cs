﻿using System;
using Bootstrap.StructureMap;
using Manufacturing.DataCollector.Datasources.Simulation;
using Manufacturing.Framework.DataPusher;
using StructureMap;
using StructureMap.Configuration.DSL;
using StructureMap.Graph;
using StructureMap.Pipeline;

namespace Manufacturing.DataPusher
{
    public class DataPusherContainer : IStructureMapRegistration
    {
        public void Register(IContainer container)
        {
            container.Configure(x => x.Scan(y =>
            {
                y.TheCallingAssembly();
                y.SingleImplementationsOfInterface().OnAddedPluginTypes(z => z.LifecycleIs(new TransientLifecycle()));
                y.ExcludeType<IDataPusher>();
                x.AddRegistry(new DataPusherRegistry());

                x.For<RandomDatasource>().LifecycleIs<SingletonLifecycle>();
            }));
            
        }
    }

    public class DataPusherRegistry : Registry
    {
        public DataPusherRegistry()
        {
            For<IDataPusher>().Use<EventHubsDataPusher>();
        }
    }
}
