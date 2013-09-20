using LINQPad;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Linq2Oracle.LinqPad
{
    sealed class GroupContextWrapper<C, T, TKey, TElement>
        where T : DbEntity
        where C : class,new()
    {
        readonly GroupingContext<C, T, TKey, TElement> _group;
        public GroupContextWrapper(GroupingContext<C, T, TKey, TElement> group)
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
