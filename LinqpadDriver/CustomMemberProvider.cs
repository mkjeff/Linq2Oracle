using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using LINQPad;

namespace Linq2Oracle.LinqPad
{
    static class CustomMemberProvider
    {
        static readonly Dictionary<Type, (string[], Type[], Func<object, object>[])> _cache = new Dictionary<Type, (string[], Type[], Func<object, object>[])>();
        static CustomMemberProvider() { }

        static readonly MethodInfo propGetterMaker = typeof(CustomMemberProvider).GetMethod("GetPropertyGetterDelegate", BindingFlags.Static | BindingFlags.NonPublic);
        static Func<object, object> GetPropertyGetterDelegate<T, TProperty>(PropertyInfo pi)
        {
            //if (pi.PropertyType.IsValueType)
            //{
            var getter = (Func<T, TProperty>)Delegate.CreateDelegate(typeof(Func<T, TProperty>), pi.GetGetMethod());
            return @this => (object)getter((T)@this);
            //}
            //else
            //{
            //    var getter = (Func<T, object>)Delegate.CreateDelegate(typeof(Func<T, object>), pi.GetGetMethod());
            //    return @this => getter((T)@this);
            //}
        }

        static Func<object, object> GetPropertyGetter(PropertyInfo property)
        {
            // @this => (object)((T)entity).PropertyOfT
            var entity = Expression.Parameter(typeof(object), "entity");
            var entityProperty = Expression.Property(Expression.Convert(entity, property.DeclaringType), property);

            var lambda = Expression.Lambda<Func<object, object>>(
                property.PropertyType.IsValueType
                ? Expression.Convert(entityProperty, typeof(object))
                : (Expression)entityProperty, entity);
            return lambda.Compile();
        }

        internal static ICustomMemberProvider GetProvider(DbEntity entity)
        {
            var type = entity.GetType();
            if (!_cache.TryGetValue(type, out var typeinfo))
            {
                var props =
                    (from p in type.GetProperties()
                     let attr = p.GetCustomAttribute<ColumnAttribute>()
                     where attr != null
                     orderby attr.IsPrimarykey descending, p.Name
                     select new
                     {
                         Name = p.Name + (attr.IsPrimarykey ? " *" : string.Empty),
                         p.PropertyType,
                         //PropertyGetter = GetPropertyGetter(p)
                         PropertyGetter = (Func<object, object>)propGetterMaker.MakeGenericMethod(p.DeclaringType, p.PropertyType).Invoke(null, new object[] { p })
                     }).ToArray();
                typeinfo = (
                    props.Select(p => p.Name).ToArray(),
                    props.Select(p => p.PropertyType).ToArray(),
                    props.Select(p => p.PropertyGetter).ToArray());
                _cache.Add(type, typeinfo);
            }
            return new EntityCustomMemberProvider(typeinfo, entity);
        }
        sealed class EntityCustomMemberProvider : ICustomMemberProvider
        {
            readonly (string[] names, Type[] types, Func<object, object>[] getters) _typeinfo;
            readonly DbEntity _obj;

            internal EntityCustomMemberProvider((string[] names, Type[] types, Func<object, object>[] getters) typeinfo, DbEntity obj)
            {
                _typeinfo = typeinfo;
                _obj = obj;
            }

            public IEnumerable<string> GetNames() { return _typeinfo.names; }
            public IEnumerable<Type> GetTypes() { return _typeinfo.types; }
            public IEnumerable<object> GetValues() { return _typeinfo.getters.Select(f => f(_obj)); }
        }
    }
}
