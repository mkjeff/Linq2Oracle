using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Linq4Oracle {
    sealed class ExpressionCache<T> where T : class {
        private readonly ConcurrentDictionary<CacheKey, T> m_storage = new ConcurrentDictionary<CacheKey, T>();

        public T Get<TKey>(TKey expr, string file, int line, Func<TKey, T> creator) where TKey : Expression
        {
            return m_storage.GetOrAdd(new CacheKey(expr,file,line), k => creator((TKey)k.Expression));
        }

        private class CacheKey {
            internal readonly Expression Expression;
            readonly string _filename;
            readonly int _line;
                internal CacheKey(Expression expr, string filename, int line)
            {
                Expression = expr;
                _filename = filename;
                _line = line;
            }

            public override int GetHashCode() {
                return _filename.GetHashCode() ^ _line.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                CacheKey other = obj as CacheKey;
                if (other == null) return false;

                return _filename.Equals(other._filename) && _line == other._line;
            }
        }
    }
}
