using System.Collections.Generic;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;

namespace Manufacturing.Framework.DataPusher
{
    public interface IDataPusher
    {
        void PushRecords(IEnumerable<DatasourceRecord> records);
    }
}
