using Npgsql;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace CVD
{
    public class HighFrequencyTickProcessor : IDisposable
    {
        private readonly string _connectionString;
        private readonly int _batchSize;
        private readonly bool _useBulkInsert;
        private readonly int _maxQueueSize;

        // Add connection pool management
        private readonly SemaphoreSlim _dbConnectionSemaphore;
        private const int MAX_CONCURRENT_DB_CONNECTIONS = 10; // Limit concurrent connections

        private readonly BlockingCollection<RawTick> _tickQueue;
        private readonly Task _processingTask;
        private readonly CancellationTokenSource _processingCts;

        private bool _disposed = false;
        private long _totalTicksProcessed = 0;
        private long _totalBatchesProcessed = 0;
        private long _failedTicks = 0;
        private long _queueOverflows = 0;
        private long _successfulInsertions = 0;

        public event Action<string> OnLogMessage;

        public HighFrequencyTickProcessor(string connectionString,
            bool useBulkInsert = true,
            int batchSize = 500,
            int maxQueueSize = 10000,
            int maxConcurrentConnections = 10)
        {
            _connectionString = connectionString;
            _useBulkInsert = useBulkInsert;
            _batchSize = batchSize;
            _maxQueueSize = maxQueueSize;

            // Initialize connection pool semaphore
            _dbConnectionSemaphore = new SemaphoreSlim(maxConcurrentConnections, maxConcurrentConnections);

            _tickQueue = new BlockingCollection<RawTick>(new ConcurrentQueue<RawTick>(), _maxQueueSize);
            _processingCts = new CancellationTokenSource();
            _processingTask = Task.Run(() => ProcessTicksBackground(_processingCts.Token));

            LogMessage($"[Line 48] HighFrequencyTickProcessor initialized in {(_useBulkInsert ? "BULK" : "SINGLE")} mode with {maxConcurrentConnections} max connections");
        }

        public bool StoreRawTick(RawTick tick)
        {
            if (_disposed)
            {
                LogMessage("[Line 55] ERROR: Attempted to store tick after disposal");
                return false;
            }

            try
            {
                if (_tickQueue.TryAdd(tick))
                {
                    Interlocked.Increment(ref _totalTicksProcessed);

                    // Log every 1000th tick to show progress
                    if (Interlocked.Read(ref _totalTicksProcessed) % 1000 == 0)
                    {
                        LogMessage($"[Line 66] Successfully queued tick #{_totalTicksProcessed:N0} - Instrument: {tick.InstrumentKey}, Price: {tick.Price}");
                    }

                    return true;
                }
                else
                {
                    Interlocked.Increment(ref _queueOverflows);
                    LogMessage("[Line 73] WARNING: Failed to add tick to queue - queue full");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _failedTicks);
                LogMessage($"[Line 79] ERROR: Failed to queue tick - {ex.Message}");
                return false;
            }
        }

        public async Task StoreRawTickAsync(RawTick tick)
        {
            if (_disposed)
            {
                LogMessage("[Line 87] ERROR: Attempted to store tick after disposal");
                return;
            }

            if (!StoreRawTick(tick))
            {
                throw new InvalidOperationException("Failed to queue tick - queue full");
            }
            await Task.CompletedTask;
        }

        private async Task ProcessTicksBackground(CancellationToken cancellationToken)
        {
            var batch = new List<RawTick>();
            long batchCounter = 0;

            LogMessage("[Line 102] Background processing started successfully");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Use a more efficient batching approach
                    var tick = _tickQueue.Take(cancellationToken);
                    batch.Add(tick);

                    // Process batch if we have enough items or after a timeout
                    if (batch.Count >= _batchSize)
                    {
                        batchCounter++;
                        var batchToProcess = batch;
                        batch = new List<RawTick>();

                        LogMessage($"[Line 116] Starting batch #{batchCounter} with {batchToProcess.Count} ticks");
                        _ = Task.Run(() => ProcessBatchWithThrottling(batchToProcess, batchCounter, cancellationToken), cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    LogMessage("[Line 121] Background processing cancelled");
                    break;
                }
                catch (Exception ex)
                {
                    LogMessage($"[Line 125] CRITICAL: Background processing error - {ex.Message}");
                    await Task.Delay(100, cancellationToken);
                }
            }

            // Process remaining batch on shutdown
            if (batch.Count > 0)
            {
                batchCounter++;
                LogMessage($"[Line 133] Processing final batch #{batchCounter} with {batch.Count} ticks during shutdown");
                await ProcessBatchWithThrottling(batch, batchCounter, CancellationToken.None);
            }

            LogMessage("[Line 137] Background processing completed");
        }

        private async Task ProcessBatchWithThrottling(List<RawTick> batch, long batchNumber, CancellationToken cancellationToken)
        {
            if (batch.Count == 0) return;

            try
            {
                LogMessage($"[Line 145] Waiting for database connection slot for batch #{batchNumber}");

                // Wait for available connection slot
                await _dbConnectionSemaphore.WaitAsync(cancellationToken);

                LogMessage($"[Line 149] Acquired database connection for batch #{batchNumber}, starting insertion");

                bool retrySuccess = await TryProcessBatchWithRetry(batch, batchNumber);
                if (!retrySuccess)
                {
                    LogMessage($"[Line 153] Batch #{batchNumber} failed all retries, writing to fallback storage");
                    WriteToFallbackStorageWithMarker(batch, batchNumber);
                }
                else
                {
                    Interlocked.Add(ref _successfulInsertions, batch.Count);
                    LogMessage($"[Line 159] ✅ SUCCESS: Batch #{batchNumber} successfully inserted {batch.Count} rows. Total successful: {_successfulInsertions:N0}");
                }
            }
            finally
            {
                // Always release the semaphore
                _dbConnectionSemaphore.Release();
                LogMessage($"[Line 166] Released database connection for batch #{batchNumber}");
            }
        }

        private async Task<bool> TryProcessBatchWithRetry(List<RawTick> batch, long batchNumber)
        {
            const int maxRetries = 2;

            for (int retry = 0; retry < maxRetries; retry++)
            {
                NpgsqlConnection conn = null;
                NpgsqlBinaryImporter writer = null;

                try
                {
                    LogMessage($"[Line 179] Attempt {retry + 1}/{maxRetries} for batch #{batchNumber}");

                    conn = new NpgsqlConnection(_connectionString);
                    await conn.OpenAsync();

                    LogMessage($"[Line 184] Database connection opened for batch #{batchNumber}");

                    const string sql = @"COPY raw_ticks (ts, instrument_key, instrument_name, price, size, bid_price, bid_qty, ask_price, ask_qty, oi, cvd, order_imbalance, instrument_type, source) 
FROM STDIN (FORMAT BINARY)";

                    writer = await conn.BeginBinaryImportAsync(sql);
                    LogMessage($"[Line 190] Binary import started for batch #{batchNumber}");

                    int rowCount = 0;
                    foreach (var tick in batch)
                    {
                        await writer.StartRowAsync();
                        await writer.WriteAsync(tick.Ts);
                        await writer.WriteAsync(tick.InstrumentKey);
                        await writer.WriteAsync(tick.InstrumentName, NpgsqlTypes.NpgsqlDbType.Text);
                        await writer.WriteAsync(tick.Price);
                        await writer.WriteAsync(tick.Size);
                        await writer.WriteAsync(tick.BidPrice ?? (object)DBNull.Value, NpgsqlTypes.NpgsqlDbType.Double);
                        await writer.WriteAsync(tick.BidQty ?? (object)DBNull.Value, NpgsqlTypes.NpgsqlDbType.Bigint);
                        await writer.WriteAsync(tick.AskPrice ?? (object)DBNull.Value, NpgsqlTypes.NpgsqlDbType.Double);
                        await writer.WriteAsync(tick.AskQty ?? (object)DBNull.Value, NpgsqlTypes.NpgsqlDbType.Bigint);
                        await writer.WriteAsync(tick.OI ?? (object)DBNull.Value, NpgsqlTypes.NpgsqlDbType.Double);
                        await writer.WriteAsync(tick.Cvd ?? (object)DBNull.Value, NpgsqlTypes.NpgsqlDbType.Double);
                        await writer.WriteAsync(tick.OrderImbalance ?? (object)DBNull.Value, NpgsqlTypes.NpgsqlDbType.Double);
                        await writer.WriteAsync("STOCK");
                        await writer.WriteAsync("websocket");

                        rowCount++;

                        // Log progress for large batches
                        if (batch.Count > 1000 && rowCount % 500 == 0)
                        {
                            LogMessage($"[Line 211] Batch #{batchNumber}: Processed {rowCount}/{batch.Count} rows");
                        }
                    }

                    LogMessage($"[Line 215] Batch #{batchNumber}: All {batch.Count} rows prepared, starting database commit");

                    await writer.CompleteAsync();
                    Interlocked.Increment(ref _totalBatchesProcessed);

                    LogMessage($"[Line 220] ✅ Database commit successful for batch #{batchNumber}");

                    if (_totalBatchesProcessed % 10 == 0) // Log more frequently
                    {
                        var stats = GetPerformanceStats();
                        LogMessage($"[Line 225] 📊 PROGRESS: {_totalBatchesProcessed} batches, {_totalTicksProcessed:N0} ticks, Queue: {stats.queueSize}, Successful: {_successfulInsertions:N0}, Failed: {_failedTicks}");
                    }

                    return true;
                }
                catch (Exception ex)
                {
                    LogMessage($"[Line 231] Retry {retry + 1}/{maxRetries} failed for batch #{batchNumber}: {ex.Message}");

                    if (retry == maxRetries - 1)
                    {
                        Interlocked.Add(ref _failedTicks, batch.Count);
                        LogMessage($"[Line 235] ❌ CRITICAL: All retries failed for batch #{batchNumber} with {batch.Count} ticks");
                        return false;
                    }

                    // Exponential backoff
                    int delay = 1000 * (int)Math.Pow(2, retry);
                    LogMessage($"[Line 241] Waiting {delay}ms before retry #{retry + 2} for batch #{batchNumber}");
                    await Task.Delay(delay);
                }
                finally
                {
                    try
                    {
                        writer?.Dispose();
                    }
                    catch { /* Ignore disposal errors */ }

                    try
                    {
                        conn?.Close();
                        conn?.Dispose();
                    }
                    catch { /* Ignore disposal errors */ }
                }
            }

            return false;
        }

        private void WriteToFallbackStorageWithMarker(List<RawTick> batch, long batchNumber)
        {
            try
            {
                string fallbackFile = $"fallback_ticks_{DateTime.Now:yyyyMMdd}.csv";
                var lines = new List<string>();

                // Add header if file doesn't exist
                if (!File.Exists(fallbackFile))
                {
                    lines.Add("ts,instrument_key,instrument_name,price,size,bid_price,bid_qty,ask_price,ask_qty,oi,cvd,order_imbalance,instrument_type,source,marker");
                }

                foreach (var tick in batch)
                {
                    string line = $"{tick.Ts:yyyy-MM-dd HH:mm:ss.fff}," +
                                 $"{tick.InstrumentKey}," +
                                 $"{tick.InstrumentName}," +
                                 $"{tick.Price}," +
                                 $"{tick.Size}," +
                                 $"{tick.BidPrice?.ToString() ?? "NULL"}," +
                                 $"{tick.BidQty?.ToString() ?? "NULL"}," +
                                 $"{tick.AskPrice?.ToString() ?? "NULL"}," +
                                 $"{tick.AskQty?.ToString() ?? "NULL"}," +
                                 $"{tick.OI?.ToString() ?? "NULL"}," +
                                 $"{tick.Cvd?.ToString() ?? "NULL"}," +
                                 $"{tick.OrderImbalance?.ToString() ?? "NULL"}," +
                                 $"STOCK,websocket,DATA_INCONSISTENT_DB_INSERT_FAILED";
                    lines.Add(line);
                }

                File.AppendAllLines(fallbackFile, lines);
                LogMessage($"[Line 283] ✅ Written batch #{batchNumber} with {batch.Count} ticks to fallback storage");
            }
            catch (Exception ex)
            {
                LogMessage($"[Line 286] ❌ CRITICAL: Fallback storage also failed for batch #{batchNumber} - {ex.Message}");
            }
        }

        public (long totalTicks, long totalBatches, long failedTicks, long queueOverflows, int queueSize) GetPerformanceStats()
        {
            return (
                Interlocked.Read(ref _totalTicksProcessed),
                Interlocked.Read(ref _totalBatchesProcessed),
                Interlocked.Read(ref _failedTicks),
                Interlocked.Read(ref _queueOverflows),
                _tickQueue.Count
            );
        }

        private void LogMessage(string message)
        {
            OnLogMessage?.Invoke($"[TickProcessor] {DateTime.Now:HH:mm:ss.fff} - {message}");
        }

        public void Dispose()
        {
            if (_disposed) return;

            _disposed = true;
            LogMessage("[Line 309] Disposing tick processor...");

            _processingCts?.Cancel();

            try
            {
                _processingTask?.Wait(TimeSpan.FromSeconds(10));
                LogMessage("[Line 316] Background task stopped successfully");
            }
            catch (AggregateException ex)
            {
                LogMessage($"[Line 319] Exception during background task stop: {ex.Message}");
            }

            _tickQueue?.CompleteAdding();

            // Process any remaining items with throttling
            var remainingBatch = new List<RawTick>();
            while (_tickQueue.TryTake(out var tick))
            {
                remainingBatch.Add(tick);
            }
            if (remainingBatch.Count > 0)
            {
                LogMessage($"[Line 330] Processing {remainingBatch.Count} remaining ticks during disposal");
                ProcessBatchWithThrottling(remainingBatch, 9999, CancellationToken.None).Wait(TimeSpan.FromSeconds(10));
            }

            _tickQueue?.Dispose();
            _processingCts?.Dispose();
            _dbConnectionSemaphore?.Dispose();

            var stats = GetPerformanceStats();
            LogMessage($"[Line 338] 📊 FINAL STATS: {stats.totalTicks:N0} ticks, {stats.totalBatches} batches, {_successfulInsertions:N0} successful, {stats.failedTicks} failed, {stats.queueOverflows} overflows");

            LogMessage("[Line 340] ✅ Tick processor disposed successfully");
        }
    }
}