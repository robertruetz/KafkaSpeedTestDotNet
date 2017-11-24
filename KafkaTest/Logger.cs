using System;
using System.Threading;

namespace KafkaTest
{
    public interface ILogger
    {
        void WriteLog(string message);
    }

    public class Logger : ILogger
    {
        private string _logFilePath;
        private string _logFormatString = "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'";
        private int _usingResource;
        private string _name;

        public string LogFilePath
        {
            get
            {
                return _logFilePath;
            }
            set
            {
                _logFilePath = value;
            }
        }

        public string Name { get => _name; set => _name = value; }

        public Logger()
        {
        }

        public Logger(string name)
        {
            Name = name;
        }

        public void WriteLog(string message)
        {
            var logMessage = $"{DateTime.UtcNow.ToString(_logFormatString)} {message}";
            try
            {
                if (0 == Interlocked.Exchange(ref _usingResource, 1))
                {
                    if (string.IsNullOrEmpty(LogFilePath))
                    {
                        Console.WriteLine(logMessage);
                    }
                    Interlocked.Exchange(ref _usingResource, 0);
                }
            }
            catch (Exception)
            {
                Console.WriteLine("An error occurred writing a log message.");
            }

            //TODO: Future Implement some kind of file logger
        }

        public void WriteLogDebug(string message, params object[] args)
        {
            if (args != null && args.Length > 0)
                message = string.Format(message, args);
            var logMessage = $"DEBUG {Name}: {message}";
            this.WriteLog(logMessage);
        }

        public void WriteLogInfo(string message, params object[] args)
        {
            if (args != null && args.Length > 0)
                message = string.Format(message, args);
            var logMessage = $"INFO {Name}: {message}";
            this.WriteLog(logMessage);
        }

        public void WriteLogError(string message, params object[] args)
        {
            if (args != null && args.Length > 0)
                message = string.Format(message, args);
            var logMessage = $"ERROR {Name}: {message}";
            this.WriteLog(logMessage);
        }
    }

    public interface ILogFormatter
    {
        void WriteLogDebug(string message, params object[] args);
        void WriteLogInfo(string message, params object[] args);
        void WriteLogError(string message, params object[] args);
    }
}
