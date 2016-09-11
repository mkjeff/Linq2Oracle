using System.Collections.Generic;
using LINQPad;

namespace Linq2Oracle.LinqPad
{
    sealed class GroupContextWrapper<T, C, TKey, TElement> where T : DbEntity where C:class,new()
    {
        readonly GroupingContext< C, T, TKey, TElement> _group;
        public GroupContextWrapper(GroupingContext< C, T, TKey, TElement> group)
        {
            _group = group;
        }

        public TKey Key => _group.Key;
        public DumpContainer Elements => Util.OnDemand("Load", DeferExecute);

        IEnumerable<TElement> DeferExecute()
        {
            foreach (var element in _group)
                yield return element;
        }
    }
}
