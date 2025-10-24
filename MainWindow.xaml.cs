using Com.Upstox.Marketdatafeederv3udapi.Rpc.Proto; // optional - remove if not available
using Google.Protobuf;
using LiveCharts.Wpf;
using MongoDB.Bson;
using MongoDB.Driver;
using Npgsql;
using RestSharp;
using SharpCompress.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Shapes;
using System.Windows.Threading;

namespace CVD
{
    public partial class MainWindow : Window
    {
        private DrawCharts chartHelper;
        private HighFrequencyTickProcessor _tickProcessor;
        // ------------- CONFIG -------------
        private readonly string _connectionString = "Host=localhost;Port=5432;Username=postgres;Password=Tripleb@003;Database=marketdata" + ";Maximum Pool Size=100;Connection Idle Lifetime=30;Connection Pruning Interval=10";
        private readonly string _sqlconnectionString = "Data Source=LAPTOP-3KVKG1RR\\SQLEXPRESS;Initial Catalog=DBSED3204;Integrated Security=True";
        // ------------- END CONFIG ---------
        private Dictionary<string, List<RawTick>> _tickBuffer = new Dictionary<string, List<RawTick>>();
        private Dictionary<string, DateTime> _lastMinuteBar = new Dictionary<string, DateTime>();
        private Dictionary<string, string> _instrumentNameMap = new Dictionary<string, string>();
        private ObservableCollection<EnhancedTradeCandidate> _candidates = new ObservableCollection<EnhancedTradeCandidate>();
        private ObservableCollection<MinuteRow> _details = new ObservableCollection<MinuteRow>();
        private DispatcherTimer _watchdogTimer;
        private DateTime _lastMessageTime = DateTime.Now;
        private TradeSignalCalculator _calculator = new TradeSignalCalculator();
        private List<string> instruments = new List<string>();
        private ClientWebSocket webSocket;
        private CancellationTokenSource _cts;
        private List<DailyStockRow> dailyStockRow = new List<DailyStockRow>();
        private List<DailyFutureRow> dailyFutureRow = new List<DailyFutureRow>();
        private string accessToken;
        private MinuteBarProcessor _minuteBarProcessor;
        private readonly ConcurrentDictionary<string, double> _lastCvdCache = new ConcurrentDictionary<string, double>();

        public MainWindow()
        {
            InitializeComponent();
            // Initialize collections first
            _tickBuffer = new Dictionary<string, List<RawTick>>();
            _lastMinuteBar = new Dictionary<string, DateTime>();

            // Initialize processors
            InitializeProcessors();

            // Initialize watchdog timer
            _watchdogTimer = new DispatcherTimer();
            _watchdogTimer.Interval = TimeSpan.FromMinutes(2); // Check every 2 minutes
            _watchdogTimer.Tick += WatchdogTimer_Tick;
            _watchdogTimer.Start();

            // UI setup
            SummaryGrid.ItemsSource = _candidates;
            DetailGrid.ItemsSource = _details;
            TimeframeCombo.SelectedIndex = 0;

            // seed data and UI defaults
            SeedSampleData();
            DbStatus.Text = "Not connected";
            ConnectionStatus.Text = "Disconnected";
            StatusText.Text = "Idle";
            Timetext.Text = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            InitializeCvdCache();

            chartHelper = new DrawCharts(SymbolCombo, TimeframeCombo, DatePickerBox, PriceChart, CvdChart);
            chartHelper.LoadSymbols();
        }
        private async void LoadChart_Click(object sender, RoutedEventArgs e)
        {
            await chartHelper.LoadChartAsync();
        }

        private async void WatchdogTimer_Tick(object sender, EventArgs e)
        {
            // If no messages received in 5 minutes but WebSocket appears connected, force reconnect
            if (webSocket?.State == WebSocketState.Open && (DateTime.Now - _lastMessageTime).TotalMinutes >= 5)
            {
                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText("🕵️ WATCHDOG: No messages in 5 minutes. Force reconnecting...\n");
                });

                await ReconnectWebSocket();
            }

            _lastMessageTime = DateTime.Now; // Update timer
        }
        #region minutebar processor
        private void InitializeProcessors()
        {
            // Initialize tick processor
            _tickProcessor = new HighFrequencyTickProcessor(
                connectionString: _connectionString,
                useBulkInsert: true,
                batchSize: 500,
                maxQueueSize: 10000);

            // Initialize minute bar processor
            _minuteBarProcessor = new MinuteBarProcessor(_connectionString);

            // Subscribe to events
            _tickProcessor.OnLogMessage += OnTickProcessorLog;
            _minuteBarProcessor.OnLogMessage += OnMinuteBarProcessorLog;

            // Start status update timer (only once)
            _statusUpdateTimer = new DispatcherTimer();
            _statusUpdateTimer.Interval = TimeSpan.FromMilliseconds(500);
            _statusUpdateTimer.Tick += UpdateStatusPanel;
            _statusUpdateTimer.Start();
        }
        private void OnMinuteBarProcessorLog(string message)
        {
            Dispatcher.Invoke(() =>
            {
                // Add to insertion log with [MINUTE] prefix
                var currentText = InsertionLog.Text;
                if (currentText.Length > 1000)
                {
                    currentText = string.Join(Environment.NewLine,
                        currentText.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries)
                                  .Reverse()
                                  .Take(20)
                                  .Reverse());
                }

                InsertionLog.Text = currentText + Environment.NewLine + message;

                var scrollViewer = GetChildOfType<ScrollViewer>(InsertionLog);
                scrollViewer?.ScrollToBottom();
            });
        }

        // Update your existing methods
        private async Task AddTickAndAggregate(RawTick tick)
        {
            if (!_tickBuffer.ContainsKey(tick.InstrumentKey))
                _tickBuffer[tick.InstrumentKey] = new List<RawTick>();
            _tickBuffer[tick.InstrumentKey].Add(tick);

            var currentMinute = new DateTime(tick.Ts.Year, tick.Ts.Month, tick.Ts.Day, tick.Ts.Hour, tick.Ts.Minute, 0);
            if (!_lastMinuteBar.ContainsKey(tick.InstrumentKey))
                _lastMinuteBar[tick.InstrumentKey] = currentMinute;

            // Check if buffer crossed into next minute
            if (currentMinute > _lastMinuteBar[tick.InstrumentKey])
            {
                var ticks = _tickBuffer[tick.InstrumentKey];
                var bar = AggregateToMinute(ticks);
                if (bar != null)
                {
                    await _minuteBarProcessor.StoreMinuteBarAsync(bar, tick.InstrumentKey);
                    // RecalculateCandidate(tick.InstrumentKey);
                }

                _tickBuffer[tick.InstrumentKey].Clear();
                _lastMinuteBar[tick.InstrumentKey] = currentMinute;
            }
        }

        private MinuteRow AggregateToMinute(List<RawTick> ticks)
        {
            if (ticks == null || ticks.Count == 0) return null;

            var ordered = ticks.OrderBy(t => t.Ts).ToList();
            string instrumentKey = ordered.First().InstrumentKey;
            double open = ordered.First().Price;
            double close = ordered.Last().Price;
            double high = ordered.Max(t => t.Price);
            double low = ordered.Min(t => t.Price);
            long volume = ordered.Sum(t => t.Size);
            long delta = (long)ordered.Sum(t => t.Cvd ?? 0);

            // Get last CVD from minute bar processor cache
            double lastCvd = _minuteBarProcessor.GetLastCVD(instrumentKey);
            double rolling = lastCvd + delta;

            var minuteRow = new MinuteRow
            {
                Time = new DateTime(ordered.First().Ts.Year, ordered.First().Ts.Month, ordered.First().Ts.Day, ordered.First().Ts.Hour, ordered.First().Ts.Minute, 0),
                Open = open,
                High = high,
                Low = low,
                Close = close,
                Volume = volume,
                Delta = delta,
                RollingCvd = rolling,
                OI = ordered.Last().OI ?? 0
            };

            // Update cache in minute bar processor
            _minuteBarProcessor.UpdateCvd(instrumentKey, rolling);

            return minuteRow;
        }

        // Update disposal
        //protected override void OnClosed(EventArgs e)
        //{
        //    _statusUpdateTimer?.Stop();
        //    _tickProcessor?.Dispose();
        //    _minuteBarProcessor?.Dispose();
        //    base.OnClosed(e);
        //} 
        #endregion

        #region get count of rows
        // Add these fields to MainWindow class
        private DispatcherTimer _statusUpdateTimer;

        // Initialize in constructor or loaded event


        private void OnTickProcessorLog(string message)
        {
            // Update UI thread safely
            Dispatcher.Invoke(() =>
            {
                // Add to insertion log
                var currentText = InsertionLog.Text;
                if (currentText.Length > 1000) // Keep last ~20 lines
                {
                    currentText = string.Join(Environment.NewLine,
                        currentText.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries)
                                  .Reverse()
                                  .Take(20)
                                  .Reverse());
                }

                InsertionLog.Text = currentText + Environment.NewLine + message;

                // Auto-scroll to bottom
                var scrollViewer = GetChildOfType<ScrollViewer>(InsertionLog);
                scrollViewer?.ScrollToBottom();
            });
        }

        private void UpdateStatusPanel(object sender, EventArgs e)
        {
            Dispatcher.Invoke(() =>
            {
                // Update tick processor stats
                if (_tickProcessor != null)
                {
                    var tickStats = _tickProcessor.GetPerformanceStats();

                    TickCount.Text = tickStats.totalTicks.ToString("N0");
                    FailedCount.Text = tickStats.failedTicks.ToString("N0");
                    QueueSize.Text = tickStats.queueSize.ToString("N0");

                    // Calculate success count
                    var success = tickStats.totalTicks - tickStats.failedTicks;
                    SuccessCount.Text = success.ToString("N0");

                    // Update status text based on queue activity
                    if (tickStats.queueSize > 0)
                    {
                        InsertionStatus.Text = "Processing Ticks...";
                        InsertionStatus.Foreground = Brushes.LightGreen;
                    }
                }

                // Update minute bar processor stats
                if (_minuteBarProcessor != null)
                {
                    var minuteStats = _minuteBarProcessor.GetPerformanceStats();
                    BatchCount.Text = minuteStats.totalBatches.ToString("N0"); // Show minute bar batches

                    // Update status if minute bars are processing
                    if (minuteStats.queueSize > 0)
                    {
                        InsertionStatus.Text = "Processing Minute Bars...";
                        InsertionStatus.Foreground = Brushes.Cyan;
                    }
                }

                // If both are idle
                if ((_tickProcessor?.GetPerformanceStats().queueSize ?? 0) == 0 &&
                    (_minuteBarProcessor?.GetPerformanceStats().queueSize ?? 0) == 0)
                {
                    InsertionStatus.Text = "Idle";
                    InsertionStatus.Foreground = Brushes.Yellow;
                }

                // Update main status
                var totalTicks = _tickProcessor?.GetPerformanceStats().totalTicks ?? 0;
                var totalBatches = _minuteBarProcessor?.GetPerformanceStats().totalBatches ?? 0;
                StatusText.Text = $"Ticks: {totalTicks:N0} | Minute Bars: {totalBatches:N0}";
            });
        }

        // Helper method to find ScrollViewer
        private static T GetChildOfType<T>(DependencyObject depObj) where T : DependencyObject
        {
            if (depObj == null) return null;

            for (int i = 0; i < VisualTreeHelper.GetChildrenCount(depObj); i++)
            {
                var child = VisualTreeHelper.GetChild(depObj, i);
                var result = (child as T) ?? GetChildOfType<T>(child);
                if (result != null) return result;
            }
            return null;
        }

        // Don't forget to dispose
        protected override void OnClosed(EventArgs e)
        {
            _statusUpdateTimer?.Stop();
            _tickProcessor?.Dispose();
            _minuteBarProcessor?.Dispose();
            base.OnClosed(e); // Only call this once
        }
        #endregion
        //private void InitializeTickProcessor()
        //{
        //    _tickProcessor = new HighFrequencyTickProcessor(
        //    connectionString: _connectionString,
        //    useBulkInsert: true,
        //    batchSize: 500,
        //    maxQueueSize: 10000);

        //    _tickProcessor = new HighFrequencyTickProcessor(_connectionString);
        //    _tickProcessor.OnLogMessage += OnTickProcessorLog;

        //    // Start status update timer
        //    _statusUpdateTimer = new DispatcherTimer();
        //    _statusUpdateTimer.Interval = TimeSpan.FromMilliseconds(500);
        //    _statusUpdateTimer.Tick += UpdateStatusPanel;
        //    _statusUpdateTimer.Start();

        //    _minuteBarProcessor = new MinuteBarProcessor(_connectionString);
        //    _tickBuffer = new Dictionary<string, List<RawTick>>();
        //    _lastMinuteBar = new Dictionary<string, DateTime>();

        //    // Subscribe to events
        //    _tickProcessor.OnLogMessage += OnTickProcessorLog;
        //    _minuteBarProcessor.OnLogMessage += OnMinuteBarProcessorLog;



        //}
        public void ClearCvdCache()
        {
            _lastCvdCache.Clear();
        }

        private double GetLastCVD(string key)
        {
            return _lastCvdCache.TryGetValue(key, out double lastCvd) ? lastCvd : 0;
        }

        private void UpdateCvd(string key, double rollingCvd)
        {
            _lastCvdCache.AddOrUpdate(key, rollingCvd, (k, oldValue) => rollingCvd);
        }


        public async Task InitializeCvdCache()
        {
            try
            {
                var conn = new NpgsqlConnection(_connectionString);
                await conn.OpenAsync();
                var cmd = new NpgsqlCommand("SELECT instrument_key, rolling_cvd FROM minute_bars WHERE ts >= NOW() - INTERVAL '1 day'", conn);

                var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    string key = reader.GetString(0);
                    double cvd = reader.GetDouble(1);
                    _lastCvdCache.AddOrUpdate(key, cvd, (k, oldValue) => cvd);
                }
            }
            catch (Exception ex)
            {
                // Log error but don't crash
                Console.WriteLine($"Error initializing CVD cache: {ex.Message}");
            }
        }

        private async Task EnsureTableExists()
        {
            Console.WriteLine("Starting EnsureTableExists...");

            // Simple version without TimeScaleDB features
            const string sql = @"
        -- Create raw_ticks table
        CREATE TABLE IF NOT EXISTS raw_ticks (
            ts TIMESTAMPTZ NOT NULL,
            instrument_key TEXT NOT NULL,
            instrument_name TEXT,
            price DOUBLE PRECISION NOT NULL,
            size BIGINT NOT NULL,
            bid_price DOUBLE PRECISION,
            bid_qty BIGINT,
            ask_price DOUBLE PRECISION,
            ask_qty BIGINT,
            oi DOUBLE PRECISION,
            cvd DOUBLE PRECISION,
            order_imbalance DOUBLE PRECISION,
            instrument_type TEXT NOT NULL DEFAULT 'STOCK',
            source TEXT NOT NULL DEFAULT 'websocket'
        );

        -- Create minute_bars table
        CREATE TABLE IF NOT EXISTS minute_bars (
            ts TIMESTAMPTZ NOT NULL,
            instrument_key TEXT NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume BIGINT NOT NULL,
            delta DOUBLE PRECISION,
            rolling_cvd DOUBLE PRECISION,
            oi DOUBLE PRECISION
        );

        -- Create stock_data table
        CREATE TABLE IF NOT EXISTS stock_data (
            timestamp TIMESTAMPTZ NOT NULL,
            instrument_key TEXT NOT NULL,
            instrument_name TEXT NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            volume BIGINT NOT NULL,
            oi DOUBLE PRECISION,
            cvd DOUBLE PRECISION,
            instrument_type TEXT
        );

        -- Create basic indexes
        CREATE INDEX IF NOT EXISTS idx_raw_ticks_ts ON raw_ticks (ts);
        CREATE INDEX IF NOT EXISTS idx_raw_ticks_instrument ON raw_ticks (instrument_key);
        CREATE INDEX IF NOT EXISTS idx_minute_bars_ts ON minute_bars (ts);
        CREATE INDEX IF NOT EXISTS idx_minute_bars_instrument ON minute_bars (instrument_key);
        CREATE INDEX IF NOT EXISTS idx_stock_data_ts ON stock_data (timestamp);
        CREATE INDEX IF NOT EXISTS idx_stock_data_instrument ON stock_data (instrument_key);
    ";

            try
            {
                Console.WriteLine("Opening database connection...");
                var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();
                Console.WriteLine("Database connection opened successfully.");

                var cmd = new NpgsqlCommand(sql, connection);
                cmd.CommandTimeout = 300; // 5 minutes timeout

                Console.WriteLine("Executing SQL commands...");
                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine("All tables created successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                throw;
            }
        }
        #region --- Sample / DB load ------------------------------------------------
        private void SeedSampleData()
        {
            _candidates.Clear();
            _candidates.Add(new EnhancedTradeCandidate { StockName = "INFY", SpotPrice = 0.82, FuturePrice = 0.75, VolumeStatus = "2.1x", StockCVD = 145000, RollingDelta = 18000, CVDStatus = "Rising", OpenHighLow = "Normal", VWAPStatus = "Above", TotalScore = 9, SignalStrength = "Strong" });
            _candidates.Add(new EnhancedTradeCandidate { StockName = "HDFCBANK", SpotPrice = -0.42, FuturePrice = -0.5, VolumeStatus = "1.8x", StockCVD = -95000, RollingDelta = -16000, CVDStatus = "Falling", OpenHighLow = "Open=High", VWAPStatus = "Below", TotalScore = 8, SignalStrength = "Strong" });
            _candidates.Add(new EnhancedTradeCandidate { StockName = "TCS", SpotPrice = 0.12, FuturePrice = 0.1, VolumeStatus = "1.0x", StockCVD = 12000, RollingDelta = 1000, CVDStatus = "Flat", OpenHighLow = "Open=Low", VWAPStatus = "Below", TotalScore = 3, SignalStrength = "Weak" });

            RefreshSort();
        }

        private void RefreshSort()
        {
            var sorted = _candidates.OrderByDescending(c => c.TotalScore).ToList();
            _candidates.Clear();
            foreach (var s in sorted) _candidates.Add(s);
        }

        private async void RefreshButton_Click(object sender, RoutedEventArgs e)
        {
            await LoadCandidatesFromDb();
        }

        private async Task LoadCandidatesFromDb()
        {
            try
            {
                var conn = new NpgsqlConnection(_connectionString);
                await conn.OpenAsync();
                DbStatus.Text = "Connected";
                DbStatus.Foreground = Brushes.LightGreen;

                // TODO: replace with query to get instrument list or use your instrument lists
                var symbols = new List<string> { "INFY", "HDFCBANK", "TCS" };

                var newList = new List<EnhancedTradeCandidate>();
                foreach (var sym in symbols)
                {
                    var lastBars = await LoadLastNMinutes(sym, 30);
                    if (lastBars == null || lastBars.Count == 0) continue;
                    var dailyStock = await LoadDailyStock(sym, 400);
                    var dailyFuture = await LoadDailyFuture(sym, 400);
                    var candidate = BuildCandidate(sym, lastBars, dailyStock, dailyFuture);
                    newList.Add(candidate);

                }
                SymbolCombo.ItemsSource = newList.ToList();
                _candidates.Clear();
                foreach (var c in newList.OrderByDescending(x => x.TotalScore)) _candidates.Add(c);

                SymbolCombo.ItemsSource = _candidates.Select(x => x.StockName).ToList();
                if (SymbolCombo.Items.Count > 0) SymbolCombo.SelectedIndex = 0;
            }
            catch (Exception ex)
            {
                DbStatus.Text = "DB Error";
                DbStatus.Foreground = Brushes.Orange;
                MessageBox.Show("DB load error: " + ex.Message);
            }
        }

        private async void LoadCandidatesFromDb_Click(object sender, RoutedEventArgs e)
        {
            await LoadCandidatesFromDb();
        }

        private void ApplyFilter_Click(object sender, RoutedEventArgs e)
        {
            string filter = FilterTextBox.Text?.Trim();
            if (string.IsNullOrEmpty(filter))
            {
                SummaryGrid.ItemsSource = _candidates;
                return;
            }
            //var filtered = _candidates.Where(c => c.StockName.Contains(filter, StringComparison.OrdinalIgnoreCase)).ToList();
            //SummaryGrid.ItemsSource = filtered;
        }

        private async void SummaryGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (SummaryGrid.SelectedItem is EnhancedTradeCandidate cand)
            {
                var list = await LoadLastNMinutes(cand.StockName, 30);
                _details.Clear();
                foreach (var m in list) _details.Add(m);
            }
        }
        #endregion

        #region --- DB Reads / Writes ------------------------------------------------
        private async Task<List<MinuteRow>> LoadLastNMinutes(string instrumentKey, int n)
        {
            var res = new List<MinuteRow>();
            var sql = @"SELECT ts, open, high, low, close, volume, delta, rolling_cvd, oi
                FROM minute_bars
                WHERE instrument_key = @key
                ORDER BY ts DESC
                LIMIT @n;";

            var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("key", instrumentKey);
            cmd.Parameters.AddWithValue("n", n);

            var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
            {
                var m = new MinuteRow
                {
                    Time = rdr.GetDateTime(0),
                    Open = rdr.IsDBNull(1) ? 0 : rdr.GetDouble(1),
                    High = rdr.IsDBNull(2) ? 0 : rdr.GetDouble(2),
                    Low = rdr.IsDBNull(3) ? 0 : rdr.GetDouble(3),
                    Close = rdr.IsDBNull(4) ? 0 : rdr.GetDouble(4),
                    Volume = rdr.IsDBNull(5) ? 0 : rdr.GetInt64(5),
                    Delta = rdr.IsDBNull(6) ? 0 : rdr.GetDouble(6),  // Changed from GetInt64 to GetDouble
                    RollingCvd = rdr.IsDBNull(7) ? 0 : rdr.GetDouble(7),
                    OI = rdr.IsDBNull(8) ? 0 : rdr.GetDouble(8),
                };
                res.Add(m);
            }

            res.Reverse();
            return res;
        }
        private async Task<List<DailyStockRow>> LoadDailyStock(string instrumentKey, int days)
        {
            var list = new List<DailyStockRow>();
            var sql = @"SELECT day_date, open, high, low, close, volume, delivery FROM daily_stock WHERE instrument_key = @k ORDER BY day_date DESC LIMIT @d;";
            var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();
            var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("k", instrumentKey);
            cmd.Parameters.AddWithValue("d", days);
            var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
            {
                list.Add(new DailyStockRow { DayDate = rdr.GetDateTime(0).Date, Open = rdr.GetDouble(1), High = rdr.GetDouble(2), Low = rdr.GetDouble(3), Close = rdr.GetDouble(4), Volume = rdr.GetInt64(5), Delivery = rdr.IsDBNull(6) ? 0 : rdr.GetDouble(6) });
            }
            return list;
        }

        private async Task<List<DailyFutureRow>> LoadDailyFuture(string instrumentKey, int days)
        {
            var list = new List<DailyFutureRow>();
            var sql = @"SELECT day_date, open, high, low, close, volume, oi FROM daily_future WHERE instrument_key=@k ORDER BY day_date DESC LIMIT @d;";
            var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();
            var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("k", instrumentKey);
            cmd.Parameters.AddWithValue("d", days);
            var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
            {
                list.Add(new DailyFutureRow { DayDate = rdr.GetDateTime(0).Date, FutOpen = rdr.GetDouble(1), FutHigh = rdr.GetDouble(2), FutLow = rdr.GetDouble(3), FutClose = rdr.GetDouble(4), FutVolume = rdr.GetInt64(5), OpenInterest = rdr.IsDBNull(6) ? 0 : rdr.GetInt64(6) });
            }
            return list;
        }

        private async Task StoreRawTickAsync(RawTick tick)
        {
            const string sql = @"INSERT INTO raw_ticks (ts, instrument_key, instrument_name, price, size, bid_price, bid_qty, ask_price, ask_qty, oi, cvd, order_imbalance, instrument_type, source)
VALUES (@ts, @instrumentKey, @instrumentName, @price, @size, @bidPrice, @bidQty, @askPrice, @askQty, @oi, @cvd, @orderImbalance, @instrumentType, @source);";

            using (var conn = new NpgsqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                using (var cmd = new NpgsqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("ts", tick.Ts);
                    cmd.Parameters.AddWithValue("instrumentKey", tick.InstrumentKey);
                    cmd.Parameters.AddWithValue("instrumentName", tick.InstrumentName ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("price", tick.Price);
                    cmd.Parameters.AddWithValue("size", tick.Size);
                    cmd.Parameters.AddWithValue("bidPrice", tick.BidPrice ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("bidQty", tick.BidQty ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("askPrice", tick.AskPrice ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("askQty", tick.AskQty ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("oi", tick.OI ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("cvd", tick.Cvd ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("orderImbalance", tick.OrderImbalance ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("instrumentType", "STOCK");
                    cmd.Parameters.AddWithValue("source", "websocket");

                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }
        private async Task InsertMinuteBarAsync(MinuteRow bar, string instrumentKey)
        {
            const string sql = @"INSERT INTO minute_bars (ts, instrument_key, open, high, low, close, volume, delta, rolling_cvd, oi)
VALUES (@ts, @instrumentKey, @open, @high, @low, @close, @volume, @delta, @rolling_cvd, @oi);";
            var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();
            var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("ts", bar.Time);
            cmd.Parameters.AddWithValue("instrumentKey", instrumentKey);
            cmd.Parameters.AddWithValue("open", bar.Open);
            cmd.Parameters.AddWithValue("high", bar.High);
            cmd.Parameters.AddWithValue("low", bar.Low);
            cmd.Parameters.AddWithValue("close", bar.Close);
            cmd.Parameters.AddWithValue("volume", bar.Volume);
            cmd.Parameters.AddWithValue("delta", bar.Delta);
            cmd.Parameters.AddWithValue("rolling_cvd", bar.RollingCvd);
            cmd.Parameters.AddWithValue("oi", bar.OI);
            await cmd.ExecuteNonQueryAsync();
        }
        #endregion

        #region --- Candidate builder & helpers ------------------------------------
        private EnhancedTradeCandidate BuildCandidate(string instrumentKey, List<MinuteRow> last30Mins, List<DailyStockRow> dailyStockHistory, List<DailyFutureRow> dailyFutureHistory)
        {
            var calc = _calculator;
            var window = Math.Min(calc.RollingWindowMinutes, last30Mins.Count);
            var lastWindow = last30Mins.Skip(Math.Max(0, last30Mins.Count - window)).ToList();
            var (rollingCvd, slope, rawSpike) = calc.ComputeRollingCvdAndSpike(lastWindow);
            double vwap = calc.ComputeVwap(last30Mins);
            var last = last30Mins.Last();
            var prev = last30Mins.Take(last30Mins.Count - 1).ToList();
            double volumeRatio = calc.ComputeVolumeRatio(last, prev);
            bool firstMinBreak = calc.CheckFirstMinuteBreakout(last30Mins);
            bool fiveMinBreak = calc.CheckFiveMinuteBreakout(last30Mins);
            var (near52, dist52) = calc.Check52Week(dailyStockHistory, last.Close);
            var (support, sHits, resistance, rHits) = calc.ComputeSupportResistance(dailyStockHistory);

            var (volScore, cvdScore, brScore) = calc.ComputeScores(volumeRatio, rollingCvd, slope, firstMinBreak, fiveMinBreak);
            int total = volScore + cvdScore + brScore;
            string signalStrength = total >= 7 ? "Strong" : total >= 4 ? "Neutral" : "Weak";

            var cand = new EnhancedTradeCandidate
            {
                StockName = instrumentKey,
                SpotPrice = ComputePercentChange(last.Close, dailyStockHistory.FirstOrDefault()?.Open ?? last.Close),
                FuturePrice = ComputePercentChangeFromFuture(dailyFutureHistory),
                OIChange = calc.ClassifyOIChange(GetLatestFutureOI(dailyFutureHistory), GetPrevFutureOI(dailyFutureHistory), ComputePercentChange(last.Close, dailyStockHistory.FirstOrDefault()?.Open ?? last.Close)),
                VolumeStatus = volumeRatio >= 2 ? $"{volumeRatio:F2}x Avg" : $"{volumeRatio:F2}x",
                VolumeRatio = Math.Round(volumeRatio, 2),
                StockCVD = rollingCvd,
                FuturesCVD = 0,
                RollingDelta = last.Delta,
                CVDStatus = slope > calc.CvdSlopeThreshold ? "Rising" : slope < -calc.CvdSlopeThreshold ? "Falling" : "Flat",
                FirstMinuteBreakout = firstMinBreak ? "HighCross" : "None",
                FiveMinuteBreakout = fiveMinBreak ? "HighCross" : "None",
                VWAPStatus = last.Close >= vwap ? "Above" : "Below",
                OpenHighLow = calc.CheckOpenHighLow(last30Mins.First()),
                Near52WeekHigh = near52,
                DistanceFrom52WH = dist52,
                VolumeScore = volScore,
                CVDScore = cvdScore,
                BreakoutScore = brScore,
                TotalScore = total,
                SignalStrength = signalStrength,
                Support = support.ToString("F2"),
                Resistance = resistance.ToString("F2"),
                SupportHit = sHits,
                ResistanceHit = rHits
            };

            if (rawSpike) { cand.CVDStatus += " (Spike)"; }
            return cand;
        }

        private double ComputePercentChange(double current, double open)
        {
            if (open == 0) return 0;
            return 100.0 * (current - open) / open;
        }

        private double ComputePercentChangeFromFuture(List<DailyFutureRow> future)
        {
            if (future == null || future.Count == 0) return 0;
            var today = future.First();
            if (today.FutOpen == 0) return 0;
            return 100.0 * (today.FutClose - today.FutOpen) / today.FutOpen;
        }

        private long GetLatestFutureOI(List<DailyFutureRow> fut)
        {
            if (fut == null || fut.Count == 0) return 0;
            return fut.First().OpenInterest;
        }
        private long GetPrevFutureOI(List<DailyFutureRow> fut)
        {
            if (fut == null || fut.Count < 2) return 0;
            return fut.Skip(1).First().OpenInterest;
        }
        #endregion

        
        #region --- Websocket skeleton (connect, receive, parse) ------------------
        private async Task<string> FetchAccessToken()
        {
            string UpstoxLoingStatus = "";
            try
            {
                var connectionString = "mongodb://localhost:27017";
                var client1 = new MongoClient(connectionString);
                var database = client1.GetDatabase("StockDB");
                var collection = database.GetCollection<BsonDocument>("GetAccessToken");
                var allDocuments = await collection.Find(new BsonDocument()).ToListAsync();
                foreach (var doc in allDocuments)
                {
                    accessToken = doc["Token"].AsString;
                    break;
                }

                var client = new RestClient("https://api.upstox.com/v2/");
                var request = new RestRequest("market-quote/quotes", Method.Get);
                request.AddHeader("Authorization", $"Bearer {accessToken}");
                request.AddParameter("symbol", "NSE_EQ|INE466L01038");
                RestResponse response = client.Execute(request);
                if (response.IsSuccessful) UpstoxLoingStatus = "LoggedIn";
            }
            catch (Exception ex)
            {
                OutputTextBox.AppendText($"FetchAccessToken error: {ex.Message}\n");
            }
            return UpstoxLoingStatus;
        }
        private async Task Getsymbollist()
        {
            try
            {
                var dataFetcher = new InstrumentsData(accessToken);
                var result = await dataFetcher.GetInstrumentsAsync();
                var equitylist = result.Item1;
                var fnolist = result.Item2;

                foreach (var i in equitylist) instruments.Add(i.instrument_key);
                foreach (var i in fnolist) instruments.Add(i.instrument_key);

                foreach (var symbol in instruments)
                {
                    var eqItem = equitylist.FirstOrDefault(x => x.instrument_key == symbol);
                    if (eqItem != null) { _instrumentNameMap[symbol] = eqItem.trading_symbol; continue; }

                    var fnoItem = fnolist.FirstOrDefault(x => x.instrument_key == symbol);
                    if (fnoItem != null) _instrumentNameMap[symbol] = fnoItem.trading_symbol;
                }
            }
            catch (Exception ex)
            {
                Dispatcher.Invoke(() => OutputTextBox.AppendText($"Getsymbollist error: {ex.Message}\n"));
            }
        }
        private async Task StartAsync()
        {
            var MyLoginresult = await FetchAccessToken();
            if (string.IsNullOrEmpty(MyLoginresult))
            {
                MessageBox.Show("Login Required");
                return;
            }
            await Getsymbollist();
            _cts = new CancellationTokenSource();

            try
            {
                webSocket = new ClientWebSocket { Options = { KeepAliveInterval = TimeSpan.FromSeconds(30) } };
                var wsUrl = await GetWebSocketUrlAsync();
                if (wsUrl == null) return;

                await webSocket.ConnectAsync(new Uri(wsUrl), _cts.Token);
                await SendSubscriptionAsync(instruments);

                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText($"Connected & subscribed to {instruments.Count} instruments.\n");
                    StatusText.Text = "Connected";
                    StatusText.Foreground = Brushes.Green;
                    ConnectionStatus.Text = "Connected";
                    ConnectionStatus.Foreground = Brushes.LightGreen;
                });

                _ = Task.Run(() => ReceiveMessages());
            }
            catch (Exception ex)
            {
                await Dispatcher.InvokeAsync(() => OutputTextBox.AppendText($"Error in StartAsync: {ex.Message}\n"));
            }
        }

        private async Task<string> GetWebSocketUrlAsync()
        {
            try
            {
                var client = new RestClient("https://api.upstox.com/v3");
                var request = new RestRequest("/feed/market-data-feed/authorize", Method.Get)
                    .AddHeader("Authorization", $"Bearer {accessToken}")
                    .AddHeader("Accept", "application/json")
                    .AddHeader("Api-Version", "3.0");

                var response = await client.ExecuteAsync(request);
                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText($"Authorize response: {response.Content}\n");
                });

                

                if (response.IsSuccessful)
                {
                    var json = JsonDocument.Parse(response.Content);
                    if (json.RootElement.GetProperty("status").GetString() == "success")
                        return json.RootElement.GetProperty("data").GetProperty("authorizedRedirectUri").GetString();
                }
            }
            catch (Exception ex)
            {
                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText($"GetWebSocketUrlAsync error: {ex.Message}\n");
                });

                
            }
            return null;
        }

        private async Task SendSubscriptionAsync(List<string> instrumentKeys)
        {
            try
            {
                const int batchSize = 50;
                for (int i = 0; i < instrumentKeys.Count; i += batchSize)
                {
                    var batch = instrumentKeys.Skip(i).Take(batchSize).ToList();
                    var subscriptionJson = $@"{{
                        ""guid"": ""{Guid.NewGuid()}"",
                        ""method"": ""sub"",
                        ""data"": {{
                            ""mode"": ""full_d30"",
                            ""instrumentKeys"": [{string.Join(",", batch.Select(k => $"\"{k}\""))}]
                        }}
                    }}";

                    var bytes = Encoding.UTF8.GetBytes(subscriptionJson);
                    await webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Binary, true, _cts.Token);
                    await Task.Delay(200);
                }
            }
            catch (Exception ex)
            {
                OutputTextBox.AppendText($"Error sending subscription: {ex.Message}\n");
            }
        }



        #region new code try 
        private async Task ReceiveMessages()
        {
            var buffer = new byte[64 * 1024];
            int retryCount = 0;
            const int maxRetries = 5;

            while (!_cts.IsCancellationRequested && retryCount < maxRetries)
            {
                try
                {
                    while (webSocket.State == WebSocketState.Open && !_cts.IsCancellationRequested)
                    {
                        var ms = new MemoryStream();
                        WebSocketReceiveResult result;
                        do
                        {
                            result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);
                            ms.Write(buffer, 0, result.Count);
                        }
                        while (!result.EndOfMessage && !_cts.IsCancellationRequested);

                        if (_cts.IsCancellationRequested) break;

                        ms.Position = 0;

                        switch (result.MessageType)
                        {
                            case WebSocketMessageType.Text:
                                using (var reader = new StreamReader(ms, Encoding.UTF8))
                                {
                                    var text = await reader.ReadToEndAsync();
                                    await ProcessTextMessage(text);
                                }
                                break;

                            case WebSocketMessageType.Binary:
                                try
                                {
                                    var feedResponse = FeedResponse.Parser.ParseFrom(ms);
                                    await ProcessFeedResponse(feedResponse);
                                }
                                catch (Exception ex)
                                {
                                    await Dispatcher.InvokeAsync(() => OutputTextBox.AppendText($"Binary parse error: {ex.Message}\n"));
                                }
                                break;

                            case WebSocketMessageType.Close:
                                await Dispatcher.InvokeAsync(() => OutputTextBox.AppendText($"WebSocket closed: {result.CloseStatus}\n"));
                                return;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    await Dispatcher.InvokeAsync(() => OutputTextBox.AppendText("Receive loop cancelled\n"));
                }
                catch (Exception ex)
                {
                    await Dispatcher.InvokeAsync(() => OutputTextBox.AppendText($"Receive loop error: {ex.Message}. Retrying...\n"));
                    retryCount++;
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, retryCount)));
                    if (webSocket.State != WebSocketState.Open)
                    {
                        await ReconnectWebSocket();
                    }
                }
            }

            if (retryCount >= maxRetries)
            {
                await Dispatcher.InvokeAsync(() => OutputTextBox.AppendText("Max retry attempts reached. Giving up.\n"));
            }
        }

        private async Task ReconnectWebSocket()
        {
            try
            {
                // Clean up old connection
                if (webSocket != null)
                {
                    try
                    {
                        if (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseReceived)
                        {
                            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Reconnecting", CancellationToken.None);
                        }
                        webSocket.Dispose();
                    }
                    catch (Exception ex)
                    {
                        await Dispatcher.InvokeAsync(() =>
                        {
                            OutputTextBox.AppendText($"Error closing old WebSocket: {ex.Message}\n");
                        });
                    }
                    webSocket = null;
                }

                // Create new connection
                webSocket = new ClientWebSocket
                {
                    Options = { KeepAliveInterval = TimeSpan.FromSeconds(30) }
                };

                var wsUrl = await GetWebSocketUrlAsync();
                await webSocket.ConnectAsync(new Uri(wsUrl), _cts.Token);

                // Resubscribe to instruments
                await SendSubscriptionAsync(instruments);

                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText("✅ WebSocket reconnected successfully\n");
                    ConnectionStatus.Text = "Connected";
                    ConnectionStatus.Foreground = Brushes.Green;
                });
            }
            catch (Exception ex)
            {
                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText($"❌ Reconnection failed: {ex.Message}\n");
                    ConnectionStatus.Text = "Failed";
                    ConnectionStatus.Foreground = Brushes.Red;
                });
                throw;
            }
        }
        private async Task ProcessCompleteMessage(WebSocketMessageType messageType, MemoryStream ms)
        {
            switch (messageType)
            {
                case WebSocketMessageType.Text:
                    using (var reader = new StreamReader(ms, Encoding.UTF8))
                    {
                        var text = await reader.ReadToEndAsync();
                        await ProcessTextMessage(text);
                    }
                    break;

                case WebSocketMessageType.Binary:
                    try
                    {
                        var feedResponse = FeedResponse.Parser.ParseFrom(ms);
                        await ProcessFeedResponse(feedResponse);
                    }
                    catch (Exception ex)
                    {
                        await Dispatcher.InvokeAsync(() =>
                        {
                            OutputTextBox.AppendText($"Binary parse error: {ex.Message}\n");
                        });
                    }
                    break;
            }
        }

        private async Task HandleCloseMessage(WebSocketReceiveResult result)
        {
            var closeReason = result.CloseStatus.HasValue
                ? $"{result.CloseStatus}: {result.CloseStatusDescription ?? "No reason"}"
                : "No close status provided";

            await Dispatcher.InvokeAsync(() =>
            {
                OutputTextBox.AppendText($"WebSocket closed by server: {closeReason}\n");
            });
        }

        private async Task HandleReconnection(int retryCount, int maxRetries, string reason)
        {
            await Dispatcher.InvokeAsync(() =>
                OutputTextBox.AppendText($"{reason}. Retry {retryCount}/{maxRetries}\n"));

            await CleanupWebSocket();

            // Progressive delay
            int delaySeconds = Math.Min(30, 5 * (int)Math.Pow(2, retryCount - 1));
            await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
        }
        private async Task CleanupWebSocket()
        {
            if (webSocket != null)
            {
                try
                {
                    // Don't try to close if already in a terminal state
                    if (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseReceived)
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Cleaning up", CancellationToken.None);
                    }
                }
                catch
                {
                    // Ignore errors during cleanup
                }
                finally
                {
                    webSocket.Dispose();
                    webSocket = null;
                }
            }
        }
        #endregion
        private async Task ProcessFeedResponse(FeedResponse feedResponse)
        {
            _lastMessageTime = DateTime.Now; // Update watchdog timer
            if (feedResponse?.Feeds == null) return;

            try
            {
                foreach (var kvp in feedResponse.Feeds)
                {
                    await ProcessSingleFeed(kvp.Key, kvp.Value);
                }

                await Dispatcher.InvokeAsync(() =>
                {
                    //LastScanText.Text = DateTime.Now.ToString("HH:mm:ss");
                });
            }
            catch (Exception ex)
            {
                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText($"ProcessFeedResponse error: {ex.Message}\n");
                });
            }
        }

        private async Task ProcessTextMessage(string text)
        {
            try
            {
                OutputTextBox.AppendText($"Received text message: {text}\n");
                var jsonDoc = JsonDocument.Parse(text);
                var root = jsonDoc.RootElement;

                if (root.TryGetProperty("type", out var typeElement) && typeElement.GetString() == "market_info")
                {
                    OutputTextBox.AppendText("Market info received - markets are open\n");
                }

                if (root.TryGetProperty("code", out var codeElement) && codeElement.GetInt32() == 200)
                {
                    OutputTextBox.AppendText("Subscription successful\n");
                }
            }
            catch (Exception ex)
            {
                OutputTextBox.AppendText($"Error processing text message: {ex.Message}\n");
            }
            await Task.CompletedTask;
        }

        private async Task ProcessSingleFeed(string instrumentKey, Feed feed)
        {
            try
            {
                double? bestBidPrice = null;
                double? bestAskPrice = null;
                long? bestBidQty = null;
                long? bestAskQty = null;

                if (feed?.FullFeed?.MarketFF == null) return;
                var data = feed.FullFeed.MarketFF;

                if (data.MarketLevel?.BidAskQuote != null && data.MarketLevel.BidAskQuote.Count > 0)
                {
                    var topQuote = data.MarketLevel.BidAskQuote[0];
                    bestBidPrice = topQuote.BidP;
                    bestAskPrice = topQuote.AskP;
                    bestBidQty = topQuote.BidQ;
                    bestAskQty = topQuote.AskQ;
                }

                double price = data.Ltpc?.Ltp ?? 0;
                if (price <= 0) return;

                double cvd = CalculateCVD(data);
                double? orderImbalance = CalculateOrderImbalance(feed);
                long size = data.Vtt;

                if (!_instrumentNameMap.TryGetValue(instrumentKey, out string name))
                    name = instrumentKey;

                var tick = new RawTick
                {
                    Ts = DateTime.UtcNow,
                    InstrumentKey = instrumentKey,
                    InstrumentName = name,
                    Price = price,
                    Size = size,
                    BidPrice = bestBidPrice,
                    BidQty = bestBidQty,
                    AskPrice = bestAskPrice,
                    AskQty = bestAskQty,
                    OI = data.Oi,
                    Cvd = cvd,
                    OrderImbalance = orderImbalance
                };

                await _tickProcessor.StoreRawTickAsync(tick);
                await AddTickAndAggregate(tick);
            }
            catch (Exception ex)
            {
                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText($"ProcessSingleFeed error: {ex.Message}\n");
                });
            }
        }

        public void Dispose()
        {
            _tickProcessor?.Dispose();
        }


        #region --- Charting -------------------------------------------------------
        //private async void LoadChart_Click(object sender, RoutedEventArgs e)
        //{
        //    if (SymbolCombo.SelectedItem == null) return;
        //    string sym = SymbolCombo.SelectedItem.ToString();
        //    sym = "NSE_EQ|INE075A01022";
        //    int tf = int.Parse(((System.Windows.Controls.ComboBoxItem)TimeframeCombo.SelectedItem).Content.ToString());
        //    var minutes = await LoadLastNMinutes(sym, 240);
        //    var agg = AggregateToTF(minutes, tf);
        //    DrawPriceChart(agg);
        //}

        private List<MinuteRow> AggregateToTF(List<MinuteRow> minutes, int tf)
        {
            var list = new List<MinuteRow>();
            if (!minutes.Any()) return list;
            var ordered = minutes.OrderBy(m => m.Time).ToList();
            for (int i = 0; i < ordered.Count; i += tf)
            {
                var bucket = ordered.Skip(i).Take(tf).ToList();
                if (!bucket.Any()) break;
                var b = new MinuteRow
                {
                    Time = bucket.First().Time,
                    Open = bucket.First().Open,
                    High = bucket.Max(x => x.High),
                    Low = bucket.Min(x => x.Low),
                    Close = bucket.Last().Close,
                    Volume = bucket.Sum(x => x.Volume),
                    Delta = bucket.Sum(x => x.Delta),
                    RollingCvd = bucket.Last().RollingCvd,
                    OI = bucket.Last().OI
                };
                list.Add(b);
            }
            return list;
        }
        //private void DrawPriceChart(List<MinuteRow> bars)
        //{
        //    PriceCanvas.Children.Clear();
        //    CvdCanvas.Children.Clear();
        //    if (bars == null || bars.Count == 0) return;

        //    double w = PriceCanvas.ActualWidth; if (w == 0) w = PriceCanvas.Width = 900;
        //    double h = PriceCanvas.ActualHeight; if (h == 0) h = PriceCanvas.Height = 400;
        //    double cvdh = CvdCanvas.ActualHeight; if (cvdh == 0) cvdh = CvdCanvas.Height = 140;

        //    double minP = bars.Min(b => b.Low);
        //    double maxP = bars.Max(b => b.High);
        //    double minCvd = bars.Min(b => b.RollingCvd);
        //    double maxCvd = bars.Max(b => b.RollingCvd);

        //    if (minP == maxP) { minP -= 1; maxP += 1; }
        //    if (minCvd == maxCvd) { minCvd -= 1; maxCvd += 1; }

        //    int n = bars.Count;
        //    double xStep = w / Math.Max(1, n - 1);

        //    //-----------------------------------
        //    // PRICE CANDLESTICKS
        //    //-----------------------------------
        //    double candleWidth = xStep * 0.6;
        //    for (int i = 0; i < n; i++)
        //    {
        //        var b = bars[i];
        //        double x = i * xStep;

        //        double yHigh = h - ((b.High - minP) / (maxP - minP) * h);
        //        double yLow = h - ((b.Low - minP) / (maxP - minP) * h);
        //        double yOpen = h - ((b.Open - minP) / (maxP - minP) * h);
        //        double yClose = h - ((b.Close - minP) / (maxP - minP) * h);

        //        // wick
        //        var wick = new Line
        //        {
        //            X1 = x,
        //            X2 = x,
        //            Y1 = yHigh,
        //            Y2 = yLow,
        //            Stroke = Brushes.Gray,
        //            StrokeThickness = 1
        //        };
        //        PriceCanvas.Children.Add(wick);

        //        // body
        //        var body = new Rectangle
        //        {
        //            Width = candleWidth,
        //            Height = Math.Max(1, Math.Abs(yOpen - yClose)),
        //            Fill = b.Close >= b.Open ? Brushes.LimeGreen : Brushes.Red,
        //            Stroke = Brushes.Transparent
        //        };
        //        Canvas.SetLeft(body, x - candleWidth / 2);
        //        Canvas.SetTop(body, Math.Min(yOpen, yClose));
        //        PriceCanvas.Children.Add(body);
        //    }

        //    //-----------------------------------
        //    // PRICE TREND LINE (Overlay)
        //    //-----------------------------------
        //    var priceLine = new Polyline { Stroke = Brushes.Yellow, StrokeThickness = 1.5 };
        //    for (int i = 0; i < n; i++)
        //    {
        //        var b = bars[i];
        //        double x = i * xStep;
        //        double y = h - ((b.Close - minP) / (maxP - minP) * h);
        //        priceLine.Points.Add(new Point(x, y));
        //    }
        //    PriceCanvas.Children.Add(priceLine);

        //    //-----------------------------------
        //    // CVD TREND LINE
        //    //-----------------------------------
        //    var cvdLine = new Polyline { Stroke = Brushes.Orange, StrokeThickness = 1.5 };
        //    for (int i = 0; i < n; i++)
        //    {
        //        var b = bars[i];
        //        double x = i * xStep;
        //        double y = cvdh - ((b.RollingCvd - minCvd) / (maxCvd - minCvd) * cvdh);
        //        cvdLine.Points.Add(new Point(x, y));
        //    }
        //    CvdCanvas.Children.Add(cvdLine);
        //}


        //private void DrawPriceChart(List<MinuteRow> bars)
        //{
        //    PriceCanvas.Children.Clear();
        //    CvdCanvas.Children.Clear();
        //    if (bars == null || bars.Count == 0) return;

        //    double w = PriceCanvas.ActualWidth; if (w == 0) w = PriceCanvas.Width = 900;
        //    double h = PriceCanvas.ActualHeight; if (h == 0) h = PriceCanvas.Height = 400;
        //    double cvdh = CvdCanvas.ActualHeight; if (cvdh == 0) cvdh = CvdCanvas.Height = 140;

        //    double minP = bars.Min(b => b.Low); double maxP = bars.Max(b => b.High);
        //    double minCvd = bars.Min(b => b.RollingCvd); double maxCvd = bars.Max(b => b.RollingCvd);
        //    if (minP == maxP) { minP -= 1; maxP += 1; }
        //    if (minCvd == maxCvd) { minCvd -= 1; maxCvd += 1; }

        //    int n = bars.Count;
        //    double xStep = w / Math.Max(1, n - 1);

        //    var priceLine = new Polyline { Stroke = Brushes.LightGreen, StrokeThickness = 1.5 };
        //    for (int i = 0; i < n; i++)
        //    {
        //        var b = bars[i];
        //        double x = i * xStep;
        //        double y = h - ((b.Close - minP) / (maxP - minP) * h);
        //        priceLine.Points.Add(new Point(x, y));
        //    }
        //    PriceCanvas.Children.Add(priceLine);

        //    for (int i = 0; i < n; i++)
        //    {
        //        double x = i * xStep;
        //        var b = bars[i];
        //        double yOpen = h - ((b.Open - minP) / (maxP - minP) * h);
        //        double yClose = h - ((b.Close - minP) / (maxP - minP) * h);
        //        double yHigh = h - ((b.High - minP) / (maxP - minP) * h);
        //        double yLow = h - ((b.Low - minP) / (maxP - minP) * h);

        //        var line = new Line { X1 = x, X2 = x, Y1 = yHigh, Y2 = yLow, Stroke = Brushes.Gray, StrokeThickness = 1 };
        //        PriceCanvas.Children.Add(line);
        //        var rect = new Rectangle { Width = Math.Max(2, xStep * 0.5), Height = Math.Max(1, Math.Abs(yOpen - yClose)), Stroke = Brushes.Transparent };
        //        Canvas.SetLeft(rect, x - rect.Width / 2);
        //        Canvas.SetTop(rect, Math.Min(yOpen, yClose));
        //        rect.Fill = b.Close >= b.Open ? Brushes.Green : Brushes.Red;
        //        PriceCanvas.Children.Add(rect);
        //    }

        //    var cvdLine = new Polyline { Stroke = Brushes.Orange, StrokeThickness = 1.5 };
        //    for (int i = 0; i < n; i++)
        //    {
        //        var b = bars[i];
        //        double x = i * xStep;
        //        double y = cvdh - ((b.RollingCvd - minCvd) / (maxCvd - minCvd) * cvdh);
        //        cvdLine.Points.Add(new Point(x, y));
        //    }
        //    CvdCanvas.Children.Add(cvdLine);
        //}
        #endregion


        //private async Task ProcessSingleFeed(string instrumentKey, Feed feed)
        //{
        //    try
        //    {
        //        double? bestBidPrice = null;
        //        double? bestAskPrice = null;
        //        long? bestBidQty = null;
        //        long? bestAskQty = null;

        //        if (feed?.FullFeed?.MarketFF == null) return;
        //        var data = feed.FullFeed.MarketFF;

        //        if (data.MarketLevel?.BidAskQuote != null && data.MarketLevel.BidAskQuote.Count > 0)
        //        {
        //            var topQuote = data.MarketLevel.BidAskQuote[0];
        //            bestBidPrice = topQuote.BidP;
        //            bestAskPrice = topQuote.AskP;
        //            bestBidQty = topQuote.BidQ;
        //            bestAskQty = topQuote.AskQ;
        //        }

        //        double price = data.Ltpc?.Ltp ?? 0;
        //        if (price <= 0) return;

        //        double cvd = CalculateCVD(data);
        //        double? orderImbalance = CalculateOrderImbalance(feed);
        //        long size = data.Vtt;


        //        if (!_instrumentNameMap.TryGetValue(instrumentKey, out string name))
        //            name = instrumentKey;

        //        var tick = new RawTick
        //        {
        //            Ts = DateTime.UtcNow,
        //            InstrumentKey = instrumentKey,
        //            InstrumentName = name,
        //            Price = price,
        //            Size = size,
        //            BidPrice = bestBidPrice,
        //            BidQty = bestBidQty,
        //            AskPrice = bestAskPrice,
        //            AskQty = bestAskQty,
        //            OI = data.Oi,
        //            Cvd = cvd,
        //            OrderImbalance = orderImbalance
        //        };

        //        await StoreRawTickAsync(tick);
        //        await AddTickAndAggregate(tick);
        //    }
        //    catch (Exception ex)
        //    {
        //        MessageBox.Show($"{ex.Message}\n\n{ex.StackTrace}");
        //        await Dispatcher.InvokeAsync(() => { OutputTextBox.AppendText($"ProcessSingleFeed error: {ex.Message}\n{ex.StackTrace}\n"); });
        //    }

        //}
        private double CalculateCVD(MarketFullFeed data)
        {
            double price = data.Ltpc?.Ltp ?? 0;
            double volume = data.Vtt;
            double? bestBid = null;
            double? bestAsk = null;

            if (data.MarketLevel?.BidAskQuote != null && data.MarketLevel.BidAskQuote.Count > 0)
            {
                var topQuote = data.MarketLevel.BidAskQuote[0];
                bestBid = topQuote.BidP;
                bestAsk = topQuote.AskP;
            }

            // Determine delta side based on trade price relative to bid/ask
            if (bestAsk.HasValue && Math.Abs(price - bestAsk.Value) < 0.001)
            {
                // Trade occurred at ask — buyer aggressive
                return volume;
            }
            else if (bestBid.HasValue && Math.Abs(price - bestBid.Value) < 0.001)
            {
                // Trade occurred at bid — seller aggressive
                return -volume;
            }
            else
            {
                // Mid or unknown execution — neutral
                return 0;
            }
        }



        //private async Task AddTickAndAggregate(RawTick tick)
        //{
        //    if (!_tickBuffer.ContainsKey(tick.InstrumentKey))
        //        _tickBuffer[tick.InstrumentKey] = new List<RawTick>();
        //    _tickBuffer[tick.InstrumentKey].Add(tick);

        //    var currentMinute = new DateTime(tick.Ts.Year, tick.Ts.Month, tick.Ts.Day, tick.Ts.Hour, tick.Ts.Minute, 0);
        //    if (!_lastMinuteBar.ContainsKey(tick.InstrumentKey))
        //        _lastMinuteBar[tick.InstrumentKey] = currentMinute;

        //    // check if buffer crossed into next minute
        //    if (currentMinute > _lastMinuteBar[tick.InstrumentKey])
        //    {
        //        var ticks = _tickBuffer[tick.InstrumentKey];
        //        var bar = AggregateToMinute(ticks);
        //        if (bar != null)
        //        {
        //            await InsertMinuteBarAsync(bar, tick.InstrumentKey);
        //            //RecalculateCandidate(tick.InstrumentKey);
        //        }

        //        _tickBuffer[tick.InstrumentKey].Clear();
        //        _lastMinuteBar[tick.InstrumentKey] = currentMinute;
        //    }
        //}

        //private MinuteRow AggregateToMinute(List<RawTick> ticks)
        //{
        //    if (ticks == null || ticks.Count == 0) return null;

        //    var ordered = ticks.OrderBy(t => t.Ts).ToList();
        //    string instrumentKey = ordered.First().InstrumentKey;
        //    double open = ordered.First().Price;
        //    double close = ordered.Last().Price;
        //    double high = ordered.Max(t => t.Price);
        //    double low = ordered.Min(t => t.Price);
        //    long volume = ordered.Sum(t => t.Size);
        //    long delta = (long)ordered.Sum(t => t.Cvd ?? 0);

        //    // Get last CVD from cache instead of database
        //    double lastCvd = GetLastCVD(instrumentKey);
        //    double rolling = lastCvd + delta;

        //    var minuteRow = new MinuteRow
        //    {
        //        Time = new DateTime(ordered.First().Ts.Year, ordered.First().Ts.Month, ordered.First().Ts.Day, ordered.First().Ts.Hour, ordered.First().Ts.Minute, 0),
        //        Open = open,
        //        High = high,
        //        Low = low,
        //        Close = close,
        //        Volume = volume,
        //        Delta = delta,
        //        RollingCvd = rolling,
        //        OI = ordered.Last().OI ?? 0
        //    };

        //    // Update cache with new rolling CVD value
        //    UpdateCvd(instrumentKey, rolling);

        //    return minuteRow;
        //}

        //private double GetLastCVD(string key)
        //{
        //    try
        //    {
        //        var conn = new NpgsqlConnection(_connectionString);
        //        conn.Open();
        //        var cmd = new NpgsqlCommand("SELECT rolling_cvd FROM minute_bars WHERE instrument_key=@k ORDER BY ts DESC LIMIT 1", conn);
        //        cmd.Parameters.AddWithValue("k", key);
        //        var res = cmd.ExecuteScalar();
        //        return res == null ? 0 : Convert.ToDouble(res);
        //    }
        //    catch { return 0; }
        //}

        #endregion

        #region --- Candidate Calculation & UI Refresh ----------------------
        private double? CalculateOrderImbalance(Feed feed)
        {
            if (feed?.FullFeed?.MarketFF?.MarketLevel?.BidAskQuote == null ||
                feed.FullFeed.MarketFF.MarketLevel.BidAskQuote.Count == 0)
                return null;

            var topQuote = feed.FullFeed.MarketFF.MarketLevel.BidAskQuote[0];

            // Ensure we have valid bid/ask quantities
            if (topQuote.BidQ == 0 && topQuote.AskQ == 0)
                return null;

            // Order Imbalance = (Bid Qty - Ask Qty) / (Bid Qty + Ask Qty)
            double total = topQuote.BidQ + topQuote.AskQ;
            if (total == 0) return null;

            return (topQuote.BidQ - topQuote.AskQ) / total;
        }
        private async void RecalculateCandidate(string instrumentKey)
        {
            var last30 = await LoadLastNMinutes(instrumentKey, 30);
            if (last30 == null || last30.Count == 0) return;

            var dailyStock = await LoadDailyStock(instrumentKey, 400);
            var dailyFuture = await LoadDailyFuture(instrumentKey, 400);

            var cand = BuildCandidate(instrumentKey, last30, dailyStock, dailyFuture);

            var existing = _candidates.FirstOrDefault(c => c.StockName == instrumentKey);
            if (existing != null)
            {
                int idx = _candidates.IndexOf(existing);
                _candidates[idx] = cand;
            }
            else
            {
                _candidates.Add(cand);
            }

            // sort high score on top
            var sorted = _candidates.OrderByDescending(c => c.TotalScore).ToList();
            _candidates.Clear();
            foreach (var c in sorted) _candidates.Add(c);
        }


        //private async Task ReconnectWebSocket()
        //{
        //    await CleanupWebSocket(); // Use the cleanup method

        //    try
        //    {
        //        webSocket = new ClientWebSocket
        //        {
        //            Options = {
        //        KeepAliveInterval = TimeSpan.FromSeconds(30),
        //        // Add any other options needed for your specific WebSocket server
        //    }
        //        };

        //        var wsUrl = await GetWebSocketUrlAsync();
        //        await webSocket.ConnectAsync(new Uri(wsUrl), _cts.Token);

        //        // Small delay to ensure connection is stable
        //        await Task.Delay(1000);

        //        await SendSubscriptionAsync(instruments);

        //        await Dispatcher.InvokeAsync(() =>
        //        {
        //            OutputTextBox.AppendText("✅ WebSocket reconnected successfully\n");
        //            ConnectionStatus.Text = "Connected";
        //            ConnectionStatus.Foreground = Brushes.Green;
        //        });
        //    }
        //    catch (Exception ex)
        //    {
        //        await Dispatcher.InvokeAsync(() =>
        //        {
        //            OutputTextBox.AppendText($"❌ Reconnection failed: {ex.Message}\n");
        //            ConnectionStatus.Text = "Failed";
        //            ConnectionStatus.Foreground = Brushes.Red;
        //        });
        //        throw;
        //    }
        //}
        private async void ConnectBtn_Click(object sender, RoutedEventArgs e)
        {
            await StartAsync();
        }
        #endregion
    }

    #region --- Models -------------------------------------------------------------
    public class EnhancedTradeCandidate
    {
        public string StockName { get; set; }
        public string Sector { get; set; }
        public double SpotPrice { get; set; }
        public double FuturePrice { get; set; }
        public string OIChange { get; set; }
        public string VolumeStatus { get; set; }
        public double VolumeRatio { get; set; }
        public double StockCVD { get; set; }
        public double FuturesCVD { get; set; }
        public double RollingDelta { get; set; }
        public string CVDStatus { get; set; }
        public string FirstMinuteBreakout { get; set; }
        public string FiveMinuteBreakout { get; set; }
        public string VWAPStatus { get; set; }
        public string OpenHighLow { get; set; }
        public string Near52WeekHigh { get; set; }
        public double DistanceFrom52WH { get; set; }
        public int VolumeScore { get; set; }
        public int CVDScore { get; set; }
        public int BreakoutScore { get; set; }
        public int TotalScore { get; set; }
        public string SignalStrength { get; set; }
        public string Support { get; set; }
        public string Resistance { get; set; }
        public int SupportHit { get; set; }
        public int ResistanceHit { get; set; }
    }

    public class MinuteRow
    {
        public DateTime Time { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public long Volume { get; set; }
        public double Delta { get; set; }  // Changed from long to double
        public double RollingCvd { get; set; }
        public double OI { get; set; }
    }

    public class RawTick
    {
        public DateTime Ts { get; set; }
        public string InstrumentKey { get; set; }
        public string InstrumentName { get; set; }
        public double Price { get; set; }
        public long Size { get; set; }
        public double? BidPrice { get; set; }
        public long? BidQty { get; set; }
        public double? AskPrice { get; set; }
        public long? AskQty { get; set; }
        public double? OI { get; set; }
        public double? Cvd { get; set; }
        public double? OrderImbalance { get; set; }
    }
    public class DailyStockRow
    {
        public DateTime DayDate { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public long Volume { get; set; }
        public double Delivery { get; set; }
    }

    public class DailyFutureRow
    {
        public DateTime DayDate { get; set; }
        public double FutOpen { get; set; }
        public double FutHigh { get; set; }
        public double FutLow { get; set; }
        public double FutClose { get; set; }
        public long FutVolume { get; set; }
        public long OpenInterest { get; set; }
    }
    #endregion

    #region --- TradeSignalCalculator ---------------------------------------------
    public class TradeSignalCalculator
    {
        public int RollingWindowMinutes { get; set; } = 15;
        public double VolumeMultiplierThreshold { get; set; } = 2.0;
        public long CvdSpikeThreshold { get; set; } = 20000;
        public double CvdSlopeThreshold { get; set; } = 2000;

        public (double rollingCvd, double slope, bool rawSpike) ComputeRollingCvdAndSpike(IEnumerable<MinuteRow> lastN)
        {
            var list = lastN.OrderBy(m => m.Time).ToList();
            if (!list.Any()) return (0, 0, false);
            double rolling = list.Last().RollingCvd;
            double slope = (list.Last().RollingCvd - list.First().RollingCvd) / Math.Max(1, (list.Count - 1));
            bool rawSpike = list.Any(m => Math.Abs(m.Delta) >= CvdSpikeThreshold);
            return (rolling, slope, rawSpike);
        }

        public double ComputeVwap(IEnumerable<MinuteRow> bars)
        {
            double pvSum = 0; long qtySum = 0;
            foreach (var b in bars)
            {
                double typical = (b.High + b.Low + b.Close) / 3.0;
                pvSum += typical * b.Volume;
                qtySum += b.Volume;
            }
            return qtySum == 0 ? 0 : pvSum / qtySum;
        }

        public double ComputeVolumeRatio(MinuteRow last, IEnumerable<MinuteRow> prev)
        {
            var prevList = prev.ToList();
            var avg = prevList.Any() ? prevList.Average(p => (double)p.Volume) : 0.0;
            return avg == 0 ? 0 : (double)last.Volume / avg;
        }

        public string ClassifyOIChange(long todayOi, long prevOi, double priceChangePercent)
        {
            long deltaOi = todayOi - prevOi;
            if (deltaOi > 0 && priceChangePercent > 0.1) return "Long Build";
            if (deltaOi > 0 && priceChangePercent < -0.1) return "Short Build";
            if (deltaOi < 0 && priceChangePercent > 0.1) return "Short Covering";
            if (deltaOi < 0 && priceChangePercent < -0.1) return "Long Unwinding";
            return "Neutral";
        }

        public string CheckOpenHighLow(MinuteRow firstMinuteOfDay)
        {
            if (firstMinuteOfDay == null) return "Normal";
            if (Math.Abs(firstMinuteOfDay.Open - firstMinuteOfDay.High) < 1e-6) return "Open=High";
            if (Math.Abs(firstMinuteOfDay.Open - firstMinuteOfDay.Low) < 1e-6) return "Open=Low";
            return "Normal";
        }

        public (string near52, double distancePct) Check52Week(List<DailyStockRow> history, double currentPrice)
        {
            if (history == null || history.Count == 0) return ("None", 0);
            var last365 = history.OrderByDescending(d => d.DayDate).Take(365).ToList();
            double hi52 = last365.Max(d => d.High);
            double lo52 = last365.Min(d => d.Low);
            if (currentPrice >= hi52) return ("BreakHigh", 100.0 * (currentPrice - hi52) / hi52);
            if (currentPrice <= lo52) return ("BreakLow", 100.0 * (lo52 - currentPrice) / lo52);
            double dist = Math.Min(100.0 * (hi52 - currentPrice) / hi52, 100.0 * (currentPrice - lo52) / lo52);
            return ("None", dist);
        }

        public (double support, int supportHits, double resistance, int resistanceHits) ComputeSupportResistance(List<DailyStockRow> history, double tolerancePct = 0.5)
        {
            var last90 = history.OrderByDescending(d => d.DayDate).Take(90).ToList();
            if (!last90.Any()) return (0, 0, 0, 0);
            var highs = last90.OrderByDescending(d => d.High).Take(3).Select(d => d.High).ToList();
            var lows = last90.OrderBy(d => d.Low).Take(3).Select(d => d.Low).ToList();
            double resistance = highs.Average();
            double support = lows.Average();
            int resHits = last90.Count(d => Math.Abs((d.Close - resistance) / resistance * 100.0) <= tolerancePct);
            int supHits = last90.Count(d => Math.Abs((d.Close - support) / support * 100.0) <= tolerancePct);
            return (support, supHits, resistance, resHits);
        }

        public (int volumeScore, int cvdScore, int breakoutScore) ComputeScores(double volumeRatio, double rollingCvd, double cvdSlope, bool isFirstMinBreak, bool isFiveMinBreak)
        {
            int vScore = 0, cScore = 0, bScore = 0;
            if (volumeRatio >= 2.0) vScore = 3;
            else if (volumeRatio >= 1.5) vScore = 2;
            else if (volumeRatio >= 1.2) vScore = 1;

            if (rollingCvd > 50000 && cvdSlope > 3000) cScore = 3;
            else if (rollingCvd > 20000 && cvdSlope > 1000) cScore = 2;
            else if (Math.Abs(rollingCvd) > 5000) cScore = 1;

            if (isFirstMinBreak) bScore += 2;
            if (isFiveMinBreak) bScore += 3;
            return (vScore, cScore, bScore);
        }

        public bool CheckFirstMinuteBreakout(List<MinuteRow> minutes)
        {
            if (minutes == null || minutes.Count < 2) return false;
            var first = minutes.First();
            return minutes.Skip(1).Take(4).Any(m => m.Close > first.High) || minutes.Skip(1).Take(4).Any(m => m.Close < first.Low);
        }

        public bool CheckFiveMinuteBreakout(List<MinuteRow> minutes)
        {
            if (minutes == null || minutes.Count < 5) return false;
            var first = minutes.First();
            return minutes.Take(5).Any(m => m.High > first.High) || minutes.Take(5).Any(m => m.Low < first.Low);
        }
    }
    #endregion
}
