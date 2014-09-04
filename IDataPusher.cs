using System.Collections.Generic;
using Manufacturing.Framework.Dto;

namespace Manufacturing.DataPusher
{
    public interface IDataPusher
    {
        void PushRecords(IEnumerable<DatasourceRecord> records);
    }
}
