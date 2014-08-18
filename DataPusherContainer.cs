using Bootstrap.StructureMap;
using StructureMap;

namespace Manufacturing.DataPusher
{
    public class DataPusherContainer : IStructureMapRegistration
    {
        public void Register(IContainer container)
        {
            container.Configure(x => x.Scan(y =>
            {
                y.TheCallingAssembly();
                y.SingleImplementationsOfInterface().OnAddedPluginTypes(z => z.LifecycleIs(InstanceScope.Unique));
            }));
        }
    }
}
