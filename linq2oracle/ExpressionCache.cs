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

            public Expression Expression { get; }

            public CacheKey(Expression exp)
            {
                this.Expression = exp;
            }

            int _hashCode;
            bool _hashCodeInitialized = false;

            public override int GetHashCode()
            {
                if (!this._hashCodeInitialized)
                {
                    this._hashCode = new ExpressionHasher().Hash(this.Expression);
                    this._hashCodeInitialized = true;
                }

                return this._hashCode;
            }

            public override bool Equals(object obj)
            {
                if (obj == null) return false;
                if (obj.GetType() != this.GetType()) return false;

                CacheKey other = (CacheKey)obj;
                return comparer.Compare(this.Expression, other.Expression) == 0;
            }
        }
    }
}
