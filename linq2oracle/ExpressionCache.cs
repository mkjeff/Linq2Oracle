using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Linq2Oracle
{
    sealed class ExpressionCache<T> where T : class
    {
        readonly ConcurrentDictionary<CacheKey, T> m_storage = new ConcurrentDictionary<CacheKey, T>();

        public T Get<TKey>(TKey key, Func<TKey, T> creator) where TKey : Expression 
            => m_storage.GetOrAdd(new CacheKey(key), k => creator((TKey)k.Expression));

        sealed class CacheKey
        {
            static CacheKey() { }
            static readonly IComparer<Expression> comparer = new ExpressionComparer();

            readonly Lazy<int> _hashCode;

            public Expression Expression { get; }

            public CacheKey(Expression exp)
            {
                Expression = exp;
                _hashCode = new Lazy<int>(() => new ExpressionHasher().Hash(Expression));
            }


            public override int GetHashCode() => _hashCode.Value;

            public override bool Equals(object obj)
            {
                if (obj == null) return false;
                if (obj.GetType() != GetType()) return false;

                CacheKey other = (CacheKey)obj;
                return comparer.Compare(Expression, other.Expression) == 0;
            }
        }
    }
}
