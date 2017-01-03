using System;
using System.Collections.Generic;
using System.Linq;

namespace Linq2Oracle
{
    static class EnumerableEx
    {
        public static IEnumerable<TResult> Return<TResult>(TResult value)
        {
            yield return value;
        }

        public static IEnumerable<TSource> Using<TSource, TResource>(Func<TResource> resourceFactory, Func<TResource, IEnumerable<TSource>> enumerableFactory) where TResource : IDisposable
        {
            if (resourceFactory == null)
                throw new ArgumentNullException(nameof(resourceFactory));
            if (enumerableFactory == null)
                throw new ArgumentNullException(nameof(enumerableFactory));

            return Using_(resourceFactory, enumerableFactory);

            IEnumerable<TSource> Using_(Func<TResource> factory, Func<TResource, IEnumerable<TSource>> selector)
            {
                using (var res = factory())
                    foreach (var item in selector(res))
                        yield return item;
            }
        }        

        public static bool IsEmpty<TSource>(this IEnumerable<TSource> source)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            return !source.Any();
        }

        public static void ForEach<TSource>(this IEnumerable<TSource> source, Action<TSource> onNext)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (onNext == null)
                throw new ArgumentNullException(nameof(onNext));

            foreach (var item in source)
                onNext(item);
        }

        public static IEnumerable<TSource> Concat<TSource>(params IEnumerable<TSource>[] sources)
        {
            if (sources == null)
                throw new ArgumentNullException(nameof(sources));

            return sources.Concat_();
        }

        public static IEnumerable<TSource> Concat<TSource>(IEnumerable<TSource> sources, TSource element) 
            => Concat(sources, Return(element));

        static IEnumerable<TSource> Concat_<TSource>(this IEnumerable<IEnumerable<TSource>> sources) 
            => sources.SelectMany(a => a);
    }

    static class EmptyList<T>
    {
        public static readonly IReadOnlyList<T> Instance = new List<T>(0);
    }
}
