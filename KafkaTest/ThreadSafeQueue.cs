using System.Collections.Generic;
using System.Threading;

namespace KafkaTest
{
    public class ThreadSafeQueue<T>
    {
        private Queue<T> _queue;
        private int _usingResource;

        public ThreadSafeQueue()
        {
            _queue = new Queue<T>();
            _usingResource = 0;
        }

        public void Enqueue(T data)
        {
            if (0 == Interlocked.Exchange(ref _usingResource, 1))
            {
                _queue.Enqueue(data);
                Interlocked.Exchange(ref _usingResource, 0);
            }
        }

        public bool TryDequeue(out T data)
        {
            data = default(T);
            bool success = false;
            if (0 == Interlocked.Exchange(ref _usingResource, 1))
            {
                if (_queue.Count > 0)
                {
                    data = _queue.Dequeue();
                    success = true;
                }
                Interlocked.Exchange(ref _usingResource, 0);
            }
            return success;
        }

        public int Count()
        {
            int count = 0;
            if (0 == Interlocked.Exchange(ref _usingResource, 1))
            {
                count = _queue.Count;
                Interlocked.Exchange(ref _usingResource, 0);
            }
            return count;
        }
    }
}
