using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;

public class QueryProgressObservable : IObservable<int>
{
    private readonly DuckDBConnection connection;
    private readonly List<IObserver<int>> observers;
    private int currentProgress;
    private CancellationTokenSource cancellationTokenSource;

    public QueryProgressObservable(DuckDBConnection connection)
    {
        this.connection = connection;
        observers = new List<IObserver<int>>();
        currentProgress = -1;
        cancellationTokenSource = new CancellationTokenSource();
    }

    public IDisposable Subscribe(IObserver<int> observer)
    {
        if (!observers.Contains(observer))
        {
            observers.Add(observer);
        }

        return new Unsubscriber(observers, observer);
    }

    public void Start()
    {
        Task.Run(async () =>
        {
            while (!cancellationTokenSource.Token.IsCancellationRequested)
            {
                var progress = connection.GetQueryProgress();
                if (progress != currentProgress)
                {
                    currentProgress = progress;
                    foreach (var observer in observers)
                    {
                        observer.OnNext(progress);
                    }
                }
                await Task.Delay(250);
            }
        }, cancellationTokenSource.Token);
    }

    public void Stop()
    {
        cancellationTokenSource.Cancel();
    }

    private class Unsubscriber : IDisposable
    {
        private readonly List<IObserver<int>> _observers;
        private readonly IObserver<int> _observer;

        public Unsubscriber(List<IObserver<int>> observers, IObserver<int> observer)
        {
            _observers = observers;
            _observer = observer;
        }

        public void Dispose()
        {
            if (_observer != null && _observers.Contains(_observer))
            {
                _observers.Remove(_observer);
            }
        }
    }
}