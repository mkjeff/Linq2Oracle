using LINQPad;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Linq2Oracle.LinqPad
{
    sealed class GroupContextWrapper<T, C, TKey, TElement> where T : DbEntity
    {
        readonly GroupingContext<T, C, TKey, TElement> _group;
        public GroupContextWrapper(GroupingContext<T, C, TKey, TElement> group)
        {
            _group = group;
        }

        public TKey Key { get { return _group.Key; } }
        public DumpContainer Elements { get { return Util.OnDemand("Load", DeferExecute); } }

        IEnumerable<TElement> DeferExecute()
        {
            foreach (var element in _group)
                yield return element;
        }
    }
}
