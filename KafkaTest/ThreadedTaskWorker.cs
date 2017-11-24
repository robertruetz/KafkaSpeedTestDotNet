using System.Threading;

namespace KafkaTest
{
    public interface IThreadedTaskWorker
    {
        void Run();
        void Start();
        void Stop();
        void Pause();
    }
    
    public class ThreadedTaskWorker: IThreadedTaskWorker
    {
        protected ILogger _logger;
        protected CountdownEvent _countdownEvent;
        protected ManualResetEvent _startSignal;
        protected volatile bool _running;

        public ThreadedTaskWorker(ILogger logger, CountdownEvent countDownEvent)
        {
            _countdownEvent = countDownEvent;
            _logger = logger;
            _startSignal = new ManualResetEvent(false);
            _running = false;
        }

        public virtual void Run()
        {
            _startSignal.WaitOne();
            while (_running)
            {
                // Code here
                _startSignal.WaitOne();
                Thread.Sleep(1000);
            }
            _countdownEvent.Signal();
        }

        public void Start()
        {
            _running = true;
            _startSignal.Set();
        }

        public void Stop()
        {
            _running = false;
            _startSignal.Set();
        }

        public void Pause()
        {
            _startSignal.Reset();
        }
    }
}
