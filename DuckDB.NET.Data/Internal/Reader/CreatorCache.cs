using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace DuckDB.NET.Data.Internal.Reader;

internal static class CreatorCache
{
    private static readonly ConcurrentDictionary<Type, Func<object>> creators=new ConcurrentDictionary<Type, Func<object>>();

    public static Func<object> GetCreator(Type type)
    {
        return creators.GetOrAdd(type, static t =>
        {
            return Expression.Lambda<Func<object>>(Expression.Convert(Expression.New(t), typeof(object))).Compile();
        });
    }
}
