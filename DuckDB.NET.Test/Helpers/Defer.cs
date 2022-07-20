using System;
using System.Collections.Generic;

namespace DuckDB.NET.Test.Helpers;

internal sealed class Defer : IDisposable
{
    private readonly Stack<Action> actions = new Stack<Action>();
    
    public Defer() {}

    public Defer(Action action)
    {
        AddAction(action);
    }
    
    public void AddAction(Action action) => actions.Push(action);

    public void Dispose()
    {
        while (actions.TryPop(out var action))
            action();
    }
}