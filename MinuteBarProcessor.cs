using Npgsql;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CVD
{
    public class MinuteBarProcessor : IDisposable
    {
        private readonly string _connectionString;
        private readonly int _batchSize;
        private readonly int _maxQueueSize;

        private readonly BlockingCollection<MinuteBarBatch> _barQueue;
        private readonly Task _processingTask;
        private readonly CancellationTokenSource _processingCts;
        private readonly SemaphoreSlim _dbConnectionSemaphore;
        private const int MAX_CONCURRENT_DB_CONNECTIONS = 5;

        private bool _disposed = false;
        private long _totalBarsProcessed = 0;
        private long _totalBatchesProcessed = 0;
        private long _failedBars = 0;
        private long _queueOverflows = 0;
        private long _successfulInsertions = 0;

        // CVD cache for rolling calculations
        private readonly ConcurrentDictionary<string, double> _lastCvdCache = new ConcurrentDictionary<string, double>();

        public event Action<string> OnLogMessage;

        public MinuteBarProcessor(string connectionString,
            int batchSize = 50,
            int maxQueueSize = 1000,
            int maxConcurrentConnections = 5)
        {
            _connectionString = connectionString;
            _batchSize = batchSize;
            _maxQueueSize = maxQueueSize;

            _dbConnectionSemaphore = new SemaphoreSlim(maxConcurrentConnections, maxConcurrentConnections);
            _barQueue = new BlockingCollection<MinuteBarBatch>(new ConcurrentQueue<MinuteBarBatch>(), _maxQueueSize);
            _processingCts = new CancellationTokenSource();
            _processingTask = Task.Run(() => ProcessBarsBackground(_processingCts.Token));

            LogMessage($"[MinuteBarProcessor] Initialized with batch size {batchSize}, max {maxConcurrentConnections} connections");
        }

        public bool StoreMinuteBar(MinuteRow bar, string instrumentKey)
        {
            if (_disposed)
            {
                LogMessage("[MinuteBarProcessor] ERROR: Attempted to store bar after disposal");
                return false;
            }

            try
            {
                var batchItem = new MinuteBarBatch { Bar = bar, InstrumentKey = instrumentKey };

                if (_barQueue.TryAdd(batchItem))
                {
                    Interlocked.Increment(ref _totalBarsProcessed);

                    if (_totalBarsProcessed % 100 == 0)
                    {
                        LogMessage($"[MinuteBarProcessor] Successfully queued bar #{_totalBarsProcessed:N0} - {instrumentKey}");
                    }

                    return true;
                }
                else
                {
                    Interlocked.Increment(ref _queueOverflows);
                    LogMessage("[MinuteBarProcessor] WARNING: Failed to add bar to queue - queue full");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _failedBars);
                LogMessage($"[MinuteBarProcessor] ERROR: Failed to queue bar - {ex.Message}");
                return false;
            }
        }

        public async Task StoreMinuteBarAsync(MinuteRow bar, string instrumentKey)
        {
            if (_disposed)
            {
                LogMessage("[MinuteBarProcessor] ERROR: Attempted to store bar after disposal");
                return;
            }

            if (!StoreMinuteBar(bar, instrumentKey))
            {
                throw new InvalidOperationException("Failed to queue minute bar - queue full");
            }
            await Task.CompletedTask;
        }

        private async Task ProcessBarsBackground(CancellationToken cancellationToken)
        {
            var batch = new List<MinuteBarBatch>();
            long batchCounter = 0;

            LogMessage("[MinuteBarProcessor] Background processing started");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Wait for bars with timeout
                    if (_barQueue.TryTake(out var barItem, 1000, cancellationToken))
                    {
                        batch.Add(barItem);

                        // Process batch if we have enough items
                        if (batch.Count >= _batchSize)
                        {
                            batchCounter++;
                            var batchToProcess = batch;
                            batch = new List<MinuteBarBatch>();

                            LogMessage($"[MinuteBarProcessor] Starting batch #{batchCounter} with {batchToProcess.Count} bars");
                            _ = Task.Run(() => ProcessBatchWithThrottling(batchToProcess, batchCounter, cancellationToken), cancellationToken);
                        }
                    }

                    // Process any remaining items if we've been waiting
                    if (batch.Count > 0 && batch.Count < _batchSize)
                    {
                        batchCounter++;
                        var batchToProcess = batch;
                        batch = new List<MinuteBarBatch>();

                        LogMessage($"[MinuteBarProcessor] Processing partial batch #{batchCounter} with {batchToProcess.Count} bars");
                        _ = Task.Run(() => ProcessBatchWithThrottling(batchToProcess, batchCounter, cancellationToken), cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    LogMessage("[MinuteBarProcessor] Background processing cancelled");
                    break;
                }
                catch (Exception ex)
                {
                    LogMessage($"[MinuteBarProcessor] CRITICAL: Background processing error - {ex.Message}");
                    await Task.Delay(1000, cancellationToken);
                }
            }

            // Process remaining batch on shutdown
            if (batch.Count > 0)
            {
                batchCounter++;
                LogMessage($"[MinuteBarProcessor] Processing final batch #{batchCounter} with {batch.Count} bars during shutdown");
                await ProcessBatchWithThrottling(batch, batchCounter, CancellationToken.None);
            }
        }

        private async Task ProcessBatchWithThrottling(List<MinuteBarBatch> batch, long batchNumber, CancellationToken cancellationToken)
        {
            if (batch.Count == 0) return;

            try
            {
                await _dbConnectionSemaphore.WaitAsync(cancellationToken);

                bool retrySuccess = await TryProcessBatchWithRetry(batch, batchNumber);
                if (!retrySuccess)
                {
                    LogMessage($"[MinuteBarProcessor] Batch #{batchNumber} failed all retries");
                    // You could add fallback storage here if needed
                }
                else
                {
                    Interlocked.Add(ref _successfulInsertions, batch.Count);
                    LogMessage($"[MinuteBarProcessor] ✅ SUCCESS: Batch #{batchNumber} inserted {batch.Count} bars. Total: {_successfulInsertions:N0}");
                }
            }
            finally
            {
                _dbConnectionSemaphore.Release();
            }
        }

        private async Task<bool> TryProcessBatchWithRetry(List<MinuteBarBatch> batch, long batchNumber)
        {
            const int maxRetries = 2;

            for (int retry = 0; retry < maxRetries; retry++)
            {
                NpgsqlConnection conn = null;
                NpgsqlBinaryImporter writer = null;

                try
                {
                    LogMessage($"[MinuteBarProcessor] Attempt {retry + 1}/{maxRetries} for batch #{batchNumber}");

                    conn = new NpgsqlConnection(_connectionString);
                    await conn.OpenAsync();

                    const string sql = @"COPY minute_bars (ts, instrument_key, open, high, low, close, volume, delta, rolling_cvd, oi) 
FROM STDIN (FORMAT BINARY)";

                    writer = await conn.BeginBinaryImportAsync(sql);

                    foreach (var item in batch)
                    {
                        await writer.StartRowAsync();
                        await writer.WriteAsync(item.Bar.Time);
                        await writer.WriteAsync(item.InstrumentKey);
                        await writer.WriteAsync(item.Bar.Open);
                        await writer.WriteAsync(item.Bar.High);
                        await writer.WriteAsync(item.Bar.Low);
                        await writer.WriteAsync(item.Bar.Close);
                        await writer.WriteAsync(item.Bar.Volume);
                        await writer.WriteAsync(item.Bar.Delta);
                        await writer.WriteAsync(item.Bar.RollingCvd);
                        await writer.WriteAsync(item.Bar.OI);
                    }

                    await writer.CompleteAsync();
                    Interlocked.Increment(ref _totalBatchesProcessed);

                    LogMessage($"[MinuteBarProcessor] ✅ Database commit successful for batch #{batchNumber}");

                    if (_totalBatchesProcessed % 10 == 0)
                    {
                        var stats = GetPerformanceStats();
                        LogMessage($"[MinuteBarProcessor] 📊 PROGRESS: {_totalBatchesProcessed} batches, {_totalBarsProcessed:N0} bars, Queue: {stats.queueSize}");
                    }

                    return true;
                }
                catch (Exception ex)
                {
                    LogMessage($"[MinuteBarProcessor] Retry {retry + 1}/{maxRetries} failed for batch #{batchNumber}: {ex.Message}");

                    if (retry == maxRetries - 1)
                    {
                        Interlocked.Add(ref _failedBars, batch.Count);
                        LogMessage($"[MinuteBarProcessor] ❌ CRITICAL: All retries failed for batch #{batchNumber}");
                        return false;
                    }

                    await Task.Delay(1000 * (int)Math.Pow(2, retry));
                }
                finally
                {
                    writer?.Dispose();
                    conn?.Close();
                    conn?.Dispose();
                }
            }

            return false;
        }

        // CVD cache methods
        public double GetLastCVD(string key)
        {
            return _lastCvdCache.TryGetValue(key, out double lastCvd) ? lastCvd : 0;
        }

        public void UpdateCvd(string key, double rollingCvd)
        {
            _lastCvdCache.AddOrUpdate(key, rollingCvd, (k, oldValue) => rollingCvd);
        }

        public (long totalBars, long totalBatches, long failedBars, long queueOverflows, int queueSize) GetPerformanceStats()
        {
            return (
                Interlocked.Read(ref _totalBarsProcessed),
                Interlocked.Read(ref _totalBatchesProcessed),
                Interlocked.Read(ref _failedBars),
                Interlocked.Read(ref _queueOverflows),
                _barQueue.Count
            );
        }

        private void LogMessage(string message)
        {
            OnLogMessage?.Invoke($"{DateTime.Now:HH:mm:ss.fff} - {message}");
        }

        public void Dispose()
        {
            if (_disposed) return;

            _disposed = true;
            LogMessage("[MinuteBarProcessor] Disposing...");

            _processingCts?.Cancel();

            try
            {
                _processingTask?.Wait(TimeSpan.FromSeconds(10));
            }
            catch (AggregateException) { }

            _barQueue?.CompleteAdding();

            // Process remaining items
            var remainingBatch = new List<MinuteBarBatch>();
            while (_barQueue.TryTake(out var bar))
            {
                remainingBatch.Add(bar);
            }
            if (remainingBatch.Count > 0)
            {
                ProcessBatchWithThrottling(remainingBatch, 9999, CancellationToken.None).Wait(TimeSpan.FromSeconds(5));
            }

            _barQueue?.Dispose();
            _processingCts?.Dispose();
            _dbConnectionSemaphore?.Dispose();

            var stats = GetPerformanceStats();
            LogMessage($"[MinuteBarProcessor] 📊 FINAL: {stats.totalBars:N0} bars, {stats.totalBatches} batches, {_successfulInsertions:N0} successful");

            LogMessage("[MinuteBarProcessor] ✅ Disposed successfully");
        }
    }

    // Helper class for batch processing
    public class MinuteBarBatch
    {
        public MinuteRow Bar { get; set; }
        public string InstrumentKey { get; set; }
    }
}