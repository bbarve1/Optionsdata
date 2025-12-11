using Com.Upstox.Marketdatafeederv3udapi.Rpc.Proto; // optional - remove if not available
using Google.Protobuf;
using LiveCharts.Wpf;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using Npgsql;
using RestSharp;
using SharpCompress.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
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
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Shapes;
using System.Windows.Threading;

namespace CVD
{
    public partial class MainWindow : Window
    {
        //private ScannerService _scannerService;
        //private ObservableCollection<DiversionSignal> _diversion30MinSignals;
        //private ObservableCollection<DiversionSignal> _diversion1HourSignals;
        //private ObservableCollection<BigCvdTrade> _bigCvdTrades;
        //private ObservableCollection<CvdSpikeSignal> _cvdSpikeSignals;

        private EnhancedScannerService _scannerService;
        private ObservableCollection<DiversionSignal> _diversion30MinSignals;
        private ObservableCollection<DiversionSignal> _diversion1HourSignals;
        private ObservableCollection<BigCvdTrade> _bigCvdTrades;
        private ObservableCollection<CvdSpikeSignal> _cvdSpikeSignals;
        private ObservableCollection<TrendFollowingSignal> _trendFollowingSignals;
        private ObservableCollection<ScalpingOpportunity> _scalpingOpportunities;

        private DrawCharts chartHelper;
        private HighFrequencyTickProcessor _tickProcessor;
        // ------------- CONFIG -------------
        private readonly string _connectionString = "Host=localhost;Port=5432;Username=postgres;Password=Tripleb@003;Database=marketdata" + ";Maximum Pool Size=100;Connection Idle Lifetime=30;Connection Pruning Interval=10";
        private readonly string _sqlconnectionString = "Data Source=LAPTOP-3KVKG1RR\\SQLEXPRESS;Initial Catalog=DBSED3204;Integrated Security=True";
        // ------------- END CONFIG ---------
        private Dictionary<string, List<RawTick>> _tickBuffer = new Dictionary<string, List<RawTick>>();
        //private Dictionary<string, DateTime> _lastMinuteBar = new Dictionary<string, DateTime>();
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
        private readonly ConcurrentDictionary<string, double> _lastCvdCache = new ConcurrentDictionary<string, double>();
        private DispatcherTimer _statusUpdateTimer;
        public MainWindow()
        {
            InitializeComponent();
            // Initialize collections first
            _tickBuffer = new Dictionary<string, List<RawTick>>();

            InitializeProcessors();

            // UI setup
            SummaryGrid.ItemsSource = _candidates;
            DetailGrid.ItemsSource = _details;
            TimeframeCombo.SelectedIndex = 0;

            // seed data and UI defaults
            //SeedSampleData();
            DbStatus.Text = "Not connected";
            ConnectionStatus.Text = "Disconnected";
            StatusText.Text = "Idle";
            Timetext.Text = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            InitializeCvdCache();

            chartHelper = new DrawCharts(SymbolCombo, TimeframeCombo, DatePickerBox, PriceChart, CvdChart);
            chartHelper.LoadSymbols();
            InitializeScanners();
            //PopulateEndTimeComboBox();
            Getsymbollist();
            Timecom.Items.Add(1);
            Timecom.Items.Add(5);
            Timecom.Items.Add(10);
            Timecom.Items.Add(15);
            Timecom.Items.Add(30);
            Timecom.Items.Add(60);

            //LoadInstruments();//temp combobox load
            //InitializeScanners();
        }


        #region --- Scanner Initialization & Execution ------------------------------
        private void InitializeScanners()
        {
            _scannerService = new EnhancedScannerService(_connectionString);

            _diversion30MinSignals = new ObservableCollection<DiversionSignal>();
            _diversion1HourSignals = new ObservableCollection<DiversionSignal>();
            _bigCvdTrades = new ObservableCollection<BigCvdTrade>();
            _cvdSpikeSignals = new ObservableCollection<CvdSpikeSignal>();
            _trendFollowingSignals = new ObservableCollection<TrendFollowingSignal>();
            _scalpingOpportunities = new ObservableCollection<ScalpingOpportunity>();

            // Set DataGrid sources
            DiversionGrid30Min.ItemsSource = _diversion30MinSignals;
            DiversionGrid1Hour.ItemsSource = _diversion1HourSignals;
            BigCvdGrid.ItemsSource = _bigCvdTrades;
            CvdSpikeGrid.ItemsSource = _cvdSpikeSignals;
            TrendFollowingGrid.ItemsSource = _trendFollowingSignals;
            DataGridCVD.ItemsSource = _scalpingOpportunities;
        }

        private async void ScanSignals_Click(object sender, RoutedEventArgs e)
        {
            await RunScanners();
        }

        private DateTime startIST = new DateTime(2025, 11, 12, 9, 15, 0);
        private DateTime endIST = new DateTime(2025, 11, 12, 9, 15, 0);
        private async Task RunScanners()
        {
            List<string> Newinstruments = new List<string>();
            _scannerService = new EnhancedScannerService(_connectionString);

            try
            {
                StatusText.Text = "Scanning...";
                OutputTextBox.AppendText($"Starting scanner at {DateTime.Now:HH:mm:ss}\n");

                using (var con = new NpgsqlConnection(_connectionString))
                {
                    con.Open();
                    var cmd = new NpgsqlCommand("SELECT DISTINCT instrument_name FROM raw_ticks ORDER BY instrument_name;", con);
                    var reader = cmd.ExecuteReader();
                    while (reader.Read())
                        Newinstruments.Add(reader.GetString(0));
                }

                var instruments = Newinstruments.Select(c => c).ToList();
                if (!instruments.Any())
                {
                    OutputTextBox.AppendText("No instruments to scan. Please load candidates first.\n");
                    return;
                }

                OutputTextBox.AppendText($"Scanning {instruments.Count} instruments...\n");

                var selectedDate = EndDatePicker.SelectedDate.Value;
                var selectedTime = DateTime.ParseExact("15:30:00", "HH:mm:ss", CultureInfo.InvariantCulture);


                endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                      selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

                // Run all scanners through the service
                var results = await _scannerService.RunAllScanners(instruments, startIST, endIST);

                if (results.Success)
                {
                    // Update UI with results
                    UpdateScannerGrid(_diversion30MinSignals, results.Diversion30Min);
                    UpdateScannerGrid(_diversion1HourSignals, results.Diversion1Hour);
                    UpdateScannerGrid(_bigCvdTrades, results.BigCvdTrades);
                    UpdateScannerGrid(_cvdSpikeSignals, results.CvdSpikes);
                    UpdateScannerGrid(_trendFollowingSignals, results.TrendFollowingSignals);
                    UpdateScannerGrid(_scalpingOpportunities, results.ScalpingOpportunities);

                    StatusText.Text = $"Scan complete - {DateTime.Now:HH:mm:ss}";
                    OutputTextBox.AppendText($"Scan completed. Found:\n");
                    OutputTextBox.AppendText($"- {results.Diversion30Min.Count} 30min diversions\n");
                    OutputTextBox.AppendText($"- {results.Diversion1Hour.Count} 1hr diversions\n");
                    OutputTextBox.AppendText($"- {results.BigCvdTrades.Count} big CVD trades\n");
                    OutputTextBox.AppendText($"- {results.CvdSpikes.Count} CVD spikes\n");
                    OutputTextBox.AppendText($"- {results.TrendFollowingSignals.Count} trend following signals\n");
                    OutputTextBox.AppendText($"- {results.ScalpingOpportunities.Count} scalping opportunities\n");
                }
                else
                {
                    StatusText.Text = "Scan failed";
                    OutputTextBox.AppendText($"Scanner error: {results.ErrorMessage}\n");
                }
            }
            catch (Exception ex)
            {
                StatusText.Text = "Scan failed";
                OutputTextBox.AppendText($"Scanner error: {ex.Message}\n");
            }
        }

        private void UpdateScannerGrid<T>(ObservableCollection<T> collection, List<T> newItems) where T : class
        {
            if (collection == null)
            {
                Console.WriteLine("Error: Collection is null in UpdateScannerGrid");
                return;
            }

            if (newItems == null)
            {
                newItems = new List<T>();
            }

            try
            {
                Dispatcher.Invoke(() =>
                {
                    collection.Clear();
                    foreach (var item in newItems)
                    {
                        collection.Add(item);
                    }
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating scanner grid: {ex.Message}");
            }
        }

        
        
        #endregion


        private async void LoadChart_Click(object sender, RoutedEventArgs e)
        {
            await chartHelper.LoadChartAsync();
        }

        #region --- Scanner Initialization & Execution ------------------------------
        // private void InitializeScanners()
        // {
        //     _scannerService = new ScannerService(_connectionString);

        //     _diversion30MinSignals = new ObservableCollection<DiversionSignal>();
        //     _diversion1HourSignals = new ObservableCollection<DiversionSignal>();
        //     _bigCvdTrades = new ObservableCollection<BigCvdTrade>();
        //     _cvdSpikeSignals = new ObservableCollection<CvdSpikeSignal>();

        //     // Set DataGrid sources
        //     DiversionGrid30Min.ItemsSource = _diversion30MinSignals;
        //     DiversionGrid1Hour.ItemsSource = _diversion1HourSignals;
        //     BigCvdGrid.ItemsSource = _bigCvdTrades;
        //     CvdSpikeGrid.ItemsSource = _cvdSpikeSignals;
        // }

        // private async void ScanSignals_Click(object sender, RoutedEventArgs e)
        // {
        //     await RunScanners();
        // }

        // private async Task RunScanners()
        // {
        //     List<string> Newinstruments = new List<string>();
        //     _scannerService = new ScannerService(_connectionString);
        //     try
        //     {
        //         StatusText.Text = "Scanning...";
        //         OutputTextBox.AppendText($"Starting scanner at {DateTime.Now:HH:mm:ss}\n");
        //         using (var con = new NpgsqlConnection(_connectionString))
        //         {
        //             con.Open();
        //             var cmd = new NpgsqlCommand("SELECT DISTINCT instrument_name FROM raw_ticks ORDER BY instrument_name;", con);
        //             var reader = cmd.ExecuteReader();
        //             while (reader.Read())
        //                 Newinstruments.Add(reader.GetString(0));
        //         }
        //         //_candidates
        //         var instruments = Newinstruments.Select(c => c).ToList();
        //         if (!instruments.Any())
        //         {
        //             OutputTextBox.AppendText("No instruments to scan. Please load candidates first.\n");
        //             return;
        //         }

        //         OutputTextBox.AppendText($"Scanning {instruments.Count} instruments...\n");

        //         // Run all scanners through the service
        //         var results = await _scannerService.RunAllScanners(instruments);

        //         if (results.Success)
        //         {
        //             // Update UI with results
        //             UpdateScannerGrid(_diversion30MinSignals, results.Diversion30Min);
        //             UpdateScannerGrid(_diversion1HourSignals, results.Diversion1Hour);
        //             UpdateScannerGrid(_bigCvdTrades, results.BigCvdTrades);
        //             UpdateScannerGrid(_cvdSpikeSignals, results.CvdSpikes);

        //             StatusText.Text = $"Scan complete - {DateTime.Now:HH:mm:ss}";
        //             OutputTextBox.AppendText($"Scan completed. Found:\n");
        //             OutputTextBox.AppendText($"- {results.Diversion30Min.Count} 30min diversions\n");
        //             OutputTextBox.AppendText($"- {results.Diversion1Hour.Count} 1hr diversions\n");
        //             OutputTextBox.AppendText($"- {results.BigCvdTrades.Count} big CVD trades\n");
        //             OutputTextBox.AppendText($"- {results.CvdSpikes.Count} CVD spikes\n");
        //         }
        //         else
        //         {
        //             StatusText.Text = "Scan failed";
        //             OutputTextBox.AppendText($"Scanner error: {results.ErrorMessage}\n");
        //         }
        //     }
        //     catch (Exception ex)
        //     {
        //         StatusText.Text = "Scan failed";
        //         OutputTextBox.AppendText($"Scanner error: {ex.Message}\n");
        //     }
        // }

        //private void UpdateScannerGrid<T>(ObservableCollection<T> collection, List<T> newItems) where T : class
        // {
        //     if (collection == null)
        //     {
        //         // Log the error and return early
        //         Console.WriteLine("Error: Collection is null in UpdateScannerGrid");
        //         return;
        //     }

        //     if (newItems == null)
        //     {
        //         newItems = new List<T>();
        //     }

        //     try
        //     {
        //         Dispatcher.Invoke(() =>
        //         {
        //             collection.Clear();
        //             foreach (var item in newItems)
        //             {
        //                 collection.Add(item);
        //             }
        //         });
        //     }
        //     catch (Exception ex)
        //     {
        //         Console.WriteLine($"Error updating scanner grid: {ex.Message}");
        //     }
        // }

        #endregion

        private void InitializeProcessors()
        {
            // Initialize tick processor
            _tickProcessor = new HighFrequencyTickProcessor(
                connectionString: _connectionString,
                useBulkInsert: true,
                batchSize: 500,
                maxQueueSize: 10000);



            // Start status update timer (only once)
            _statusUpdateTimer = new DispatcherTimer();
            _statusUpdateTimer.Interval = TimeSpan.FromMilliseconds(500);
            _statusUpdateTimer.Tick += UpdateStatusPanel;
            _statusUpdateTimer.Start();
        }

        #region get count of rows

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


                // If both are idle

                // Update main status
                var totalTicks = _tickProcessor?.GetPerformanceStats().totalTicks ?? 0;

                StatusText.Text = $"Ticks: {totalTicks:N0}";
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

        #endregion

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


        #region --- Sample / DB load ------------------------------------------------
        //private void SeedSampleData()
        //{
        //    _candidates.Clear();
        //    _candidates.Add(new EnhancedTradeCandidate { StockName = "INFY", SpotPrice = 0.82, FuturePrice = 0.75, VolumeStatus = "2.1x", StockCVD = 145000, RollingDelta = 18000, CVDStatus = "Rising", OpenHighLow = "Normal", VWAPStatus = "Above", TotalScore = 9, SignalStrength = "Strong" });
        //    _candidates.Add(new EnhancedTradeCandidate { StockName = "HDFCBANK", SpotPrice = -0.42, FuturePrice = -0.5, VolumeStatus = "1.8x", StockCVD = -95000, RollingDelta = -16000, CVDStatus = "Falling", OpenHighLow = "Open=High", VWAPStatus = "Below", TotalScore = 8, SignalStrength = "Strong" });
        //    _candidates.Add(new EnhancedTradeCandidate { StockName = "TCS", SpotPrice = 0.12, FuturePrice = 0.1, VolumeStatus = "1.0x", StockCVD = 12000, RollingDelta = 1000, CVDStatus = "Flat", OpenHighLow = "Open=Low", VWAPStatus = "Below", TotalScore = 3, SignalStrength = "Weak" });

        //    RefreshSort();
        //}

        private void RefreshSort()
        {
            var sorted = _candidates.OrderByDescending(c => c.TotalScore).ToList();
            _candidates.Clear();
            foreach (var s in sorted) _candidates.Add(s);
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
                var niftyce = result.Item3;
                var niftype = result.Item4;

                //foreach (var i in equitylist) instruments.Add(i.instrument_key);
                foreach (var i in fnolist) instruments.Add(i.instrument_key);
                foreach (var i in niftyce) instruments.Add(i.instrument_key);
                foreach (var i in niftype) instruments.Add(i.instrument_key);

                foreach (var symbol in instruments)
                {
                    var eqItem = equitylist.FirstOrDefault(x => x.instrument_key == symbol);
                    if (eqItem != null) { _instrumentNameMap[symbol] = eqItem.trading_symbol; continue; }

                    var fnoItem = fnolist.FirstOrDefault(x => x.instrument_key == symbol);
                    if (fnoItem != null) _instrumentNameMap[symbol] = fnoItem.trading_symbol;

                    var niftyceitem = niftyce.FirstOrDefault(x => x.instrument_key == symbol);
                    if (niftyceitem != null) _instrumentNameMap[symbol] = niftyceitem.trading_symbol;

                    var niftypeitem = niftype.FirstOrDefault(x => x.instrument_key == symbol);
                    if (niftypeitem != null) _instrumentNameMap[symbol] = niftypeitem.trading_symbol;
                }
            }
            catch (Exception ex)
            {
                Dispatcher.Invoke(() => OutputTextBox.AppendText($"Getsymbollist error: {ex.Message}\n"));
            }

            foreach (var item in _instrumentNameMap)
            {
                InstrumentCombo.Items.Add(item.Value);
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
                            ""mode"": ""full"",
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
                //await AddTickAndAggregate(tick);
            }
            catch (Exception ex)
            {
                await Dispatcher.InvokeAsync(() =>
                {
                    OutputTextBox.AppendText($"ProcessSingleFeed error: {ex.Message}\n");
                });
            }
        }

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

        private async void ConnectBtn_Click(object sender, RoutedEventArgs e)
        {
            await StartAsync();
        }
        #endregion

        private async void BackButton_Click(object sender, RoutedEventArgs e)
        {
            await RunScanners();
        }

        private async void NextButton_Click(object sender, RoutedEventArgs e)
        {
            await RunScanners();
        }



        private void EndTimeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            //UpdateEndIST();
        }
        //private void UpdateEndIST()
        //{
        //    if (EndDatePicker.SelectedDate != null && EndTimeComboBox.SelectedItem != null)
        //    {
        //        var selectedDate = EndDatePicker.SelectedDate.Value;
        //        var selectedTime = DateTime.ParseExact(EndTimeComboBox.SelectedItem.ToString(), "HH:mm:ss", CultureInfo.InvariantCulture);

        //        endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                              selectedTime.Hour, selectedTime.Minute, selectedTime.Second);
        //        UpdateTimeRangeDisplay();
        //    }
        //}
        private void UpdateTimeRangeDisplay()
        {
            //EndTimeComboBox.Text = $"Start: {startIST:yyyy-MM-dd HH:mm:ss} | End: {endIST:yyyy-MM-dd HH:mm:ss}";
        }
        //private void EndDatePicker_SelectedDateChanged(object sender, SelectionChangedEventArgs e)
        //{
        //    UpdateEndIST();
        //}

        //        private async void Search_Click(object sender, RoutedEventArgs e)
        //        {
        //            if (InstrumentCombo.SelectedItem == null)
        //            {
        //                MessageBox.Show("Please select an instrument.");
        //                return;
        //            }

        //            var selectedDate = EndDatePicker.SelectedDate.Value;
        //            var selectedTime = DateTime.ParseExact(EndTimeComboBox.SelectedItem.ToString(), "HH:mm:ss", CultureInfo.InvariantCulture);
        //            var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);

        //            startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                                  firsttime.Hour, firsttime.Minute, firsttime.Second);

        //            endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                                  selectedTime.Hour, selectedTime.Minute, selectedTime.Second);


        //            string symbol = InstrumentCombo.SelectedItem.ToString();


        //            var startUtc = startIST.ToUniversalTime();
        //            var endUtc = endIST.ToUniversalTime();
        //            int aggmin =15;
        //            var con = new NpgsqlConnection(_connectionString);
        //            await con.OpenAsync();

        //            string sql = @"
        //        WITH ticks AS (
        //        SELECT 
        //        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        //        ts,
        //        price,
        //        size,
        //        oi,

        //        -- Compute tick delta from price movement instead of using raw CVV
        //        CASE 
        //            WHEN price > LAG(price) OVER (ORDER BY ts) THEN size
        //            ELSE 0 
        //        END AS tick_buy,

        //        CASE 
        //            WHEN price < LAG(price) OVER (ORDER BY ts) THEN size
        //            ELSE 0 
        //        END AS tick_sell

        //    FROM raw_ticks
        //    WHERE instrument_name = @instrumentName
        //      AND ts BETWEEN @startUtc AND @endUtc
        //),

        //bucketed AS (
        //    SELECT 
        //        (date_trunc('minute', ts_ist) 
        //         - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS candle_time,

        //        price, size, oi,
        //        ts_ist,
        //        tick_buy,
        //        tick_sell,

        //        row_number() OVER (
        //            PARTITION BY (date_trunc('minute', ts_ist) 
        //               - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))
        //            ORDER BY ts_ist ASC
        //        ) AS rn_open,

        //        row_number() OVER (
        //            PARTITION BY (date_trunc('minute', ts_ist) 
        //               - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))
        //            ORDER BY ts_ist DESC
        //        ) AS rn_close
        //    FROM ticks
        //),

        //candles AS (
        //    SELECT 
        //        candle_time,

        //        MAX(price) FILTER (WHERE rn_open = 1)  AS open_price,
        //        MAX(price)                             AS high_price,
        //        MIN(price)                             AS low_price,
        //        MAX(price) FILTER (WHERE rn_close = 1) AS close_price,

        //        SUM(size) AS total_volume,

        //        SUM(tick_buy)  AS total_buy_delta,
        //        SUM(tick_sell) AS total_sell_delta,

        //        MAX(oi) AS last_oi
        //    FROM bucketed
        //    GROUP BY candle_time
        //),

        //final AS (
        //    SELECT
        //        candle_time,
        //        open_price, high_price, low_price, close_price,
        //        total_volume,

        //        (total_buy_delta - total_sell_delta) AS total_delta,

        //        total_buy_delta,
        //        total_sell_delta,

        //        CASE WHEN (total_buy_delta + total_sell_delta) > 0
        //             THEN total_buy_delta * 100.0 / (total_buy_delta + total_sell_delta)
        //             ELSE 0 END AS buy_strength_pct,

        //        CASE WHEN (total_buy_delta + total_sell_delta) > 0
        //             THEN total_sell_delta * 100.0 / (total_buy_delta + total_sell_delta)
        //             ELSE 0 END AS sell_strength_pct,

        //        last_oi,
        //        last_oi - LAG(last_oi) OVER (ORDER BY candle_time) AS oi_change,
        //        close_price - LAG(close_price) OVER (ORDER BY candle_time) AS price_change,

        //        CASE 
        //            WHEN (close_price > open_price AND (total_buy_delta - total_sell_delta) < 0)
        //                THEN 'Bearish Divergence'
        //            WHEN (close_price < open_price AND (total_buy_delta - total_sell_delta) > 0)
        //                THEN 'Bullish Divergence'
        //            ELSE 'None'
        //        END AS divergence,

        //        CASE 
        //            WHEN last_oi > LAG(last_oi) OVER (ORDER BY candle_time)
        //             AND close_price > LAG(close_price) OVER (ORDER BY candle_time)
        //                THEN 'LONG BUILD-UP'

        //            WHEN last_oi > LAG(last_oi) OVER (ORDER BY candle_time)
        //             AND close_price < LAG(close_price) OVER (ORDER BY candle_time)
        //                THEN 'SHORT BUILD-UP'

        //            WHEN last_oi < LAG(last_oi) OVER (ORDER BY candle_time)
        //             AND close_price > LAG(close_price) OVER (ORDER BY candle_time)
        //                THEN 'SHORT COVERING'

        //            WHEN last_oi < LAG(last_oi) OVER (ORDER BY candle_time)
        //             AND close_price < LAG(close_price) OVER (ORDER BY candle_time)
        //                THEN 'LONG UNWINDING'

        //            ELSE 'NEUTRAL'
        //        END AS signal_type,

        //        CASE 
        //            WHEN (total_buy_delta - total_sell_delta) > 0 THEN 'Buyers Aggressive'
        //            WHEN (total_buy_delta - total_sell_delta) < 0 THEN 'Sellers Aggressive'
        //            ELSE 'Flat'
        //        END AS delta_sentiment

        //    FROM candles
        //)

        //SELECT *
        //FROM final
        //ORDER BY candle_time ASC;";

        //            var cmd = new NpgsqlCommand(sql, con);
        //            cmd.Parameters.AddWithValue("instrumentName", symbol);
        //            cmd.Parameters.AddWithValue("startUtc", startUtc);
        //            cmd.Parameters.AddWithValue("endUtc", endUtc);
        //            cmd.Parameters.AddWithValue("aggmin", aggmin); // integer interval, e.g. 5,15,30

        //            var dt = new DataTable();

        //            using (var reader = await cmd.ExecuteReaderAsync())
        //            {
        //                dt.Load(reader);
        //            }
        //            ReplaceWithLakhFormat(dt);
        //            DataGridCVD.ItemsSource = dt.DefaultView;
        //            MessageBox.Show("Done");
        //        }

        private async void ScanCvdSpikesButton(object sender, RoutedEventArgs e)
        {

        }
        private async void Search_Click(object sender, RoutedEventArgs e)
        {
            if (InstrumentCombo.SelectedItem == null)
            {
                MessageBox.Show("Please select an instrument.");
                return;
            }
            if (Timecom.SelectedItem == null)
            {
                MessageBox.Show("Please select Time Interval.");
                return;
            }

            try
            {
                var selectedDate = EndDatePicker.SelectedDate.Value;

                var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);
                var selectedTime = DateTime.ParseExact("15:30:00", "HH:mm:ss", CultureInfo.InvariantCulture);

                startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                      firsttime.Hour, firsttime.Minute, firsttime.Second);

                endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                      selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

                string symbol = InstrumentCombo.SelectedItem.ToString();
                int aggmin = (int)Timecom.SelectedItem; // keep as you had it
                var startUtc = startIST.ToUniversalTime();
                var endUtc = endIST.ToUniversalTime();

                using (var con = new NpgsqlConnection(_connectionString))
                {
                    await con.OpenAsync();

                    string sql = @"
WITH ticks AS (
    SELECT 
        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        ts,
        price,
        size,
        oi,
        order_imbalance,   -- ADDED
        CASE 
            WHEN price > LAG(price) OVER (ORDER BY ts) THEN size
            ELSE 0 
        END AS tick_buy,
        CASE 
            WHEN price < LAG(price) OVER (ORDER BY ts) THEN size
            ELSE 0 
        END AS tick_sell
    FROM raw_ticks
    WHERE instrument_name = @instrumentName
      AND ts BETWEEN @startUtc AND @endUtc
),

bucketed AS (
    SELECT 
        (date_trunc('minute', ts_ist) 
         - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS candle_time,
        price, size, oi, ts_ist,
        tick_buy, tick_sell,
        order_imbalance,   -- ADDED
        row_number() OVER (
            PARTITION BY (date_trunc('minute', ts_ist) 
               - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))
            ORDER BY ts_ist ASC
        ) AS rn_open,
        row_number() OVER (
            PARTITION BY (date_trunc('minute', ts_ist) 
               - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))
            ORDER BY ts_ist DESC
        ) AS rn_close
    FROM ticks
),

candles AS (
    SELECT 
        candle_time,
        MAX(price) FILTER (WHERE rn_open = 1)  AS open_price,
        MAX(price)                             AS high_price,
        MIN(price)                             AS low_price,
        MAX(price) FILTER (WHERE rn_close = 1) AS close_price,
        SUM(size) AS total_volume,
        SUM(tick_buy)  AS total_buy_delta,
        SUM(tick_sell) AS total_sell_delta,
        MAX(oi) AS last_oi,
        SUM(order_imbalance) AS total_order_imbalance   -- ADDED
    FROM bucketed
    GROUP BY candle_time
),

final AS (
    SELECT
        candle_time,
        open_price, high_price, low_price, close_price,
        total_volume,

        (total_buy_delta - total_sell_delta) AS total_delta,
        total_buy_delta,
        total_sell_delta,

        -- ORDER IMBALANCE (ADDED)
        total_order_imbalance,
        SUM(total_order_imbalance) OVER (ORDER BY candle_time) AS cumulative_order_imbalance,   -- ADDED

        -- Volume and Delta in Lakhs
        total_volume / 100000.0 AS volume_lakh,
        (total_buy_delta - total_sell_delta) / 100000.0 AS delta_lakh,

        -- Cumulative CVD in Lakhs
        SUM((total_buy_delta - total_sell_delta) / 100000.0) OVER (ORDER BY candle_time) AS cumulative_cvd_lakh,

        -- Open = High/Low indicators
        CASE WHEN open_price = high_price THEN 'Yes' ELSE 'No' END AS open_equals_high,
        CASE WHEN open_price = low_price THEN 'Yes' ELSE 'No' END AS open_equals_low,

        -- Candle Strength Percentage (Body as % of Range)
        CASE 
            WHEN (high_price - low_price) > 0 
            THEN ABS(close_price - open_price) * 100.0 / (high_price - low_price)
            ELSE 0 
        END AS candle_strength_pct,

        -- Buy/Sell Strength Percentage
        CASE WHEN (total_buy_delta + total_sell_delta) > 0
             THEN total_buy_delta * 100.0 / (total_buy_delta + total_sell_delta)
             ELSE 0 END AS buy_strength_pct,

        CASE WHEN (total_buy_delta + total_sell_delta) > 0
             THEN total_sell_delta * 100.0 / (total_buy_delta + total_sell_delta)
             ELSE 0 END AS sell_strength_pct,

        last_oi,
        last_oi - LAG(last_oi) OVER (ORDER BY candle_time) AS oi_change,
        close_price - LAG(close_price) OVER (ORDER BY candle_time) AS price_change,

        -- Enhanced Divergence with threshold (kept as original)
        CASE 
            WHEN (close_price > open_price AND (total_buy_delta - total_sell_delta) < -50000) 
                 AND ABS(close_price - open_price) > (high_price - low_price) * 0.3
                THEN 'Strong Bearish Divergence'
            WHEN (close_price > open_price AND (total_buy_delta - total_sell_delta) < 0) 
                THEN 'Bearish Divergence'
            WHEN (close_price < open_price AND (total_buy_delta - total_sell_delta) > 50000) 
                 AND ABS(close_price - open_price) > (high_price - low_price) * 0.3
                THEN 'Strong Bullish Divergence'
            WHEN (close_price < open_price AND (total_buy_delta - total_sell_delta) > 0) 
                THEN 'Bullish Divergence'
            ELSE 'None'
        END AS divergence,

        -- Signal Type (kept as original)
        CASE 
            WHEN last_oi > LAG(last_oi) OVER (ORDER BY candle_time)
             AND close_price > LAG(close_price) OVER (ORDER BY candle_time)
                THEN 'LONG BUILD-UP'

            WHEN last_oi > LAG(last_oi) OVER (ORDER BY candle_time)
             AND close_price < LAG(close_price) OVER (ORDER BY candle_time)
                THEN 'SHORT BUILD-UP'

            WHEN last_oi < LAG(last_oi) OVER (ORDER BY candle_time)
             AND close_price > LAG(close_price) OVER (ORDER BY candle_time)
                THEN 'SHORT COVERING'
            
            WHEN last_oi < LAG(last_oi) OVER (ORDER BY candle_time)
             AND close_price < LAG(close_price) OVER (ORDER BY candle_time)
                THEN 'LONG UNWINDING'

            ELSE 'NEUTRAL'
        END AS signal_type,

        -- Enhanced Delta Sentiment with threshold (kept as original)
        CASE 
            WHEN (total_buy_delta - total_sell_delta) > 50000 THEN 'Strong Buyers'
            WHEN (total_buy_delta - total_sell_delta) > 10000 THEN 'Buyers Aggressive'
            WHEN (total_buy_delta - total_sell_delta) < -50000 THEN 'Strong Sellers'
            WHEN (total_buy_delta - total_sell_delta) < -10000 THEN 'Sellers Aggressive'
            WHEN (total_buy_delta - total_sell_delta) > 0 THEN 'Mild Buyers'
            WHEN (total_buy_delta - total_sell_delta) < 0 THEN 'Mild Sellers'
            ELSE 'Flat'
        END AS delta_sentiment

    FROM candles
)

SELECT *
FROM final
ORDER BY candle_time ASC;
";

                    using (var cmd = new NpgsqlCommand(sql, con))
                    {
                        cmd.Parameters.AddWithValue("instrumentName", symbol);
                        cmd.Parameters.AddWithValue("startUtc", startUtc);
                        cmd.Parameters.AddWithValue("endUtc", endUtc);
                        cmd.Parameters.AddWithValue("aggmin", aggmin);

                        var dt = new DataTable();
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            dt.Load(reader);
                        }

                        DataGridCVD.ItemsSource = dt.DefaultView;
                        MessageBox.Show("Done");
                    }
                }
            }
            catch (Exception ex)
            {
                //MessageBox.Show($"Error:"  ex.Message};
            }
        }

        //private async void Search_Click(object sender, RoutedEventArgs e)
        //{
        //    if (InstrumentCombo.SelectedItem == null)
        //    {
        //        MessageBox.Show("Please select an instrument.");
        //        return;
        //    }
        //    if (Timecom.SelectedItem == null)
        //    {
        //        MessageBox.Show("Please select Time Interval.");
        //        return;
        //    }
        //    if (EndDatePicker.SelectedDate == null)
        //    {
        //        MessageBox.Show("Please select End Date.");
        //        return;
        //    }
        //    var selectedDate = EndDatePicker.SelectedDate.Value;
        //    var selectedTime = DateTime.ParseExact(EndTimeComboBox.SelectedItem.ToString(), "HH:mm:ss", CultureInfo.InvariantCulture);
        //    var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);

        //    startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                          firsttime.Hour, firsttime.Minute, firsttime.Second);

        //    endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                          selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

        //    string symbol = InstrumentCombo.SelectedItem.ToString();
        //    //string Mytime = Timecom.SelectedItem.ToString();
        //    int aggmin = (int)Timecom.SelectedItem;
        //    var startUtc = startIST.ToUniversalTime();
        //    var endUtc = endIST.ToUniversalTime();

        //    //int aggmin = 5;
        //    var con = new NpgsqlConnection(_connectionString);
        //    await con.OpenAsync();


        //    #region
        //    string sql = @"
        //    WITH ticks AS (
        //        SELECT 
        //            ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        //            ts,
        //            price,
        //            size,
        //            oi,
        //            -- Compute tick delta from price movement
        //            CASE 
        //                WHEN price > LAG(price) OVER (ORDER BY ts) THEN size
        //                ELSE 0 
        //            END AS tick_buy,
        //            CASE 
        //                WHEN price < LAG(price) OVER (ORDER BY ts) THEN size
        //                ELSE 0 
        //            END AS tick_sell
        //        FROM raw_ticks
        //        WHERE instrument_name = @instrumentName
        //          AND ts BETWEEN @startUtc AND @endUtc
        //    ),

        //    bucketed AS (
        //        SELECT 
        //            (date_trunc('minute', ts_ist) 
        //             - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS candle_time,
        //            price, size, oi,
        //            ts_ist,
        //            tick_buy,
        //            tick_sell,
        //            row_number() OVER (
        //                PARTITION BY (date_trunc('minute', ts_ist) 
        //                   - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))
        //                ORDER BY ts_ist ASC
        //            ) AS rn_open,
        //            row_number() OVER (
        //                PARTITION BY (date_trunc('minute', ts_ist) 
        //                   - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))
        //                ORDER BY ts_ist DESC
        //            ) AS rn_close
        //        FROM ticks
        //    ),

        //    candles AS (
        //        SELECT 
        //            candle_time,
        //            MAX(price) FILTER (WHERE rn_open = 1)  AS open_price,
        //            MAX(price)                             AS high_price,
        //            MIN(price)                             AS low_price,
        //            MAX(price) FILTER (WHERE rn_close = 1) AS close_price,
        //            SUM(size) AS total_volume,
        //            SUM(tick_buy)  AS total_buy_delta,
        //            SUM(tick_sell) AS total_sell_delta,
        //            MAX(oi) AS last_oi
        //        FROM bucketed
        //        GROUP BY candle_time
        //    ),

        //    final AS (
        //        SELECT
        //            candle_time,
        //            open_price, high_price, low_price, close_price,
        //            total_volume,
        //            (total_buy_delta - total_sell_delta) AS total_delta,
        //            total_buy_delta,
        //            total_sell_delta,

        //            -- Volume and Delta in Lakhs
        //            total_volume / 100000.0 AS volume_lakh,
        //            (total_buy_delta - total_sell_delta) / 100000.0 AS delta_lakh,

        //            -- Cumulative CVD in Lakhs
        //            SUM((total_buy_delta - total_sell_delta) / 100000.0) OVER (ORDER BY candle_time) AS cumulative_cvd_lakh,

        //            -- Open = High/Low indicators
        //            CASE WHEN open_price = high_price THEN 'Yes' ELSE 'No' END AS open_equals_high,
        //            CASE WHEN open_price = low_price THEN 'Yes' ELSE 'No' END AS open_equals_low,

        //            -- Candle Strength Percentage (Body as % of Range)
        //            CASE 
        //                WHEN (high_price - low_price) > 0 
        //                THEN ABS(close_price - open_price) * 100.0 / (high_price - low_price)
        //                ELSE 0 
        //            END AS candle_strength_pct,

        //            -- Buy/Sell Strength Percentage
        //            CASE WHEN (total_buy_delta + total_sell_delta) > 0
        //                 THEN total_buy_delta * 100.0 / (total_buy_delta + total_sell_delta)
        //                 ELSE 0 END AS buy_strength_pct,

        //            CASE WHEN (total_buy_delta + total_sell_delta) > 0
        //                 THEN total_sell_delta * 100.0 / (total_buy_delta + total_sell_delta)
        //                 ELSE 0 END AS sell_strength_pct,

        //            last_oi,
        //            last_oi - LAG(last_oi) OVER (ORDER BY candle_time) AS oi_change,
        //            close_price - LAG(close_price) OVER (ORDER BY candle_time) AS price_change,

        //            -- Enhanced Divergence with threshold
        //            CASE 
        //                WHEN (close_price > open_price AND (total_buy_delta - total_sell_delta) < -50000) 
        //                     AND ABS(close_price - open_price) > (high_price - low_price) * 0.3
        //                    THEN 'Strong Bearish Divergence'
        //                WHEN (close_price > open_price AND (total_buy_delta - total_sell_delta) < 0) 
        //                    THEN 'Bearish Divergence'
        //                WHEN (close_price < open_price AND (total_buy_delta - total_sell_delta) > 50000) 
        //                     AND ABS(close_price - open_price) > (high_price - low_price) * 0.3
        //                    THEN 'Strong Bullish Divergence'
        //                WHEN (close_price < open_price AND (total_buy_delta - total_sell_delta) > 0) 
        //                    THEN 'Bullish Divergence'
        //                ELSE 'None'
        //            END AS divergence,

        //            -- Signal Type
        //            CASE 
        //                WHEN last_oi > LAG(last_oi) OVER (ORDER BY candle_time)
        //                 AND close_price > LAG(close_price) OVER (ORDER BY candle_time)
        //                    THEN 'LONG BUILD-UP'

        //                WHEN last_oi > LAG(last_oi) OVER (ORDER BY candle_time)
        //                 AND close_price < LAG(close_price) OVER (ORDER BY candle_time)
        //                    THEN 'SHORT BUILD-UP'

        //                WHEN last_oi < LAG(last_oi) OVER (ORDER BY candle_time)
        //                 AND close_price > LAG(close_price) OVER (ORDER BY candle_time)
        //                    THEN 'SHORT COVERING'

        //                WHEN last_oi < LAG(last_oi) OVER (ORDER BY candle_time)
        //                 AND close_price < LAG(close_price) OVER (ORDER BY candle_time)
        //                    THEN 'LONG UNWINDING'

        //                ELSE 'NEUTRAL'
        //            END AS signal_type,

        //            -- Enhanced Delta Sentiment with threshold
        //            CASE 
        //                WHEN (total_buy_delta - total_sell_delta) > 50000 THEN 'Strong Buyers'
        //                WHEN (total_buy_delta - total_sell_delta) > 10000 THEN 'Buyers Aggressive'
        //                WHEN (total_buy_delta - total_sell_delta) < -50000 THEN 'Strong Sellers'
        //                WHEN (total_buy_delta - total_sell_delta) < -10000 THEN 'Sellers Aggressive'
        //                WHEN (total_buy_delta - total_sell_delta) > 0 THEN 'Mild Buyers'
        //                WHEN (total_buy_delta - total_sell_delta) < 0 THEN 'Mild Sellers'
        //                ELSE 'Flat'
        //            END AS delta_sentiment

        //        FROM candles
        //    )

        //    SELECT *
        //    FROM final
        //    ORDER BY candle_time ASC;";
        //    #endregion
        //    var cmd = new NpgsqlCommand(sql, con);
        //    cmd.Parameters.AddWithValue("instrumentName", symbol);
        //    cmd.Parameters.AddWithValue("startUtc", startUtc);
        //    cmd.Parameters.AddWithValue("endUtc", endUtc);
        //    cmd.Parameters.AddWithValue("aggmin", aggmin);

        //    var dt = new DataTable();

        //    using (var reader = await cmd.ExecuteReaderAsync())
        //    {
        //        dt.Load(reader);
        //    }

        //    // Remove the ReplaceWithLakhFormat call since we're now handling it in SQL
        //    DataGridCVD.ItemsSource = dt.DefaultView;
        //    MessageBox.Show("Done");
        //}
        private void ReplaceWithLakhFormat(DataTable dataTable)
        {
            try
            {
                //bool hasVolume = dataTable.Columns.Contains("total_volume");
                //bool hasDelta = dataTable.Columns.Contains("Delta");
                //bool hasOI = dataTable.Columns.Contains("OI");
                //foreach (DataColumn col in dataTable.Columns)
                //{
                //    Console.WriteLine($"- {col.ColumnName} ({col.DataType})");
                //}
                dataTable.Columns.Add("Volume_Lakh", typeof(string));
                dataTable.Columns.Add("Delta_Lakh", typeof(string));
                // Loop through all rows and replace values
                foreach (DataRow row in dataTable.Rows)
                {
                    // Convert and replace Volume
                    if (row["total_volume"] != null && !Convert.IsDBNull(row["total_volume"]) &&
                        double.TryParse(row["total_volume"].ToString(), out double volume))
                    {
                        row["Volume_Lakh"] = ConvertToLakhFormat(volume);
                    }

                    // Convert and replace Delta
                    if (row["total_delta"] != null && !Convert.IsDBNull(row["total_delta"]) &&
                        double.TryParse(row["total_delta"].ToString(), out double delta))
                    {
                        row["Delta_Lakh"] = ConvertToLakhFormat(delta);
                    }

                    // Convert and replace OI
                    //if (row["OI"] != null && !Convert.IsDBNull(row["OI"]) &&
                    //    double.TryParse(row["OI"].ToString(), out double oi))
                    //{
                    //    row["OI"] = ConvertToLakhFormat(oi);
                    //}
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error converting to lakh format: {ex.Message}");
            }
        }

        private string ConvertToLakhFormat(double number)
        {
            if (number == 0) return "0";

            double lakhValue = number / 100000.0;
            return $"{lakhValue:0.##}L";
        }
        private void ScalpingGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {

        }

        //{"RELIANCE FUT 25 NOV 25", 10.3},    // Higher weight = more impact on Nifty
        //{"INFY FUT 25 NOV 25", 3.1},
        //{"HDFCBANK FUT 25 NOV 25", 7.3},
        //{"TCS FUT 25 NOV 25", 5.4},
        //{"ICICIBANK FUT 25 NOV 25", 7.1},
        //{"RELIANCE FUT 25 NOV 25", 4.2},
        //{"BHARTIARTL FUT 25 NOV 25", 6.1},
        //{"ITC FUT 25 NOV 25", 3.5},
        //{"KOTAKBANK FUT 25 NOV 25", 3.8}

        private async void SectorCVD_Click(object sender, RoutedEventArgs e)
        {
            // Linear flow similar to your original structure
            try
            {
                if (Timecom.SelectedItem == null)
                {
                    MessageBox.Show("Please select Time Interval.");
                    return;
                }
                if (EndDatePicker.SelectedDate == null)
                {
                    MessageBox.Show("Please select End Date.");
                    return;
                }

                var selectedDate = EndDatePicker.SelectedDate.Value;
                //var selectedTime = DateTime.ParseExact(EndTimeComboBox.SelectedItem.ToString(), "HH:mm:ss", CultureInfo.InvariantCulture);
                var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);
                var selectedTime = DateTime.ParseExact("15:30:00", "HH:mm:ss", CultureInfo.InvariantCulture);

                var startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                      firsttime.Hour, firsttime.Minute, firsttime.Second);

                var endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                      selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

                int aggmin = (int)Timecom.SelectedItem;
                var startUtc = startIST.ToUniversalTime();
                var endUtc = endIST.ToUniversalTime();

                // Build instrument list from sectors dictionary
                var instrumentList = _sectors.Values.SelectMany(x => x).Distinct().ToList();
                if (!instrumentList.Any())
                {
                    MessageBox.Show("No instruments defined in sectors.");
                    return;
                }

                // Build SQL that returns per-instrument bucket aggregates
                string sql = @"
WITH tick_data AS (
    SELECT 
        instrument_name,
        ts,
        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        price,
        size,
        LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) AS prev_price
    FROM raw_ticks
    WHERE ts BETWEEN @startUtc AND @endUtc
      AND instrument_name = ANY(@instrs)
),

calculated_deltas AS (
    SELECT 
        instrument_name,
        ts_ist,
        size,
        CASE WHEN price > prev_price THEN size ELSE 0 END AS buy,
        CASE WHEN price < prev_price THEN size ELSE 0 END AS sell
    FROM tick_data
    WHERE prev_price IS NOT NULL
),

bucketed AS (
    SELECT 
        instrument_name,
        (date_trunc('minute', ts_ist) 
          - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin)) AS bucket_time,
        SUM(size) AS volume,
        SUM(buy) AS buy_delta,
        SUM(sell) AS sell_delta,
        (SUM(buy) - SUM(sell)) AS net_delta
    FROM calculated_deltas
    GROUP BY instrument_name, bucket_time
),

sector_map AS (
    SELECT * FROM (VALUES
        
        -- BANK SECTOR
        ('HDFCBANK FUT 30 DEC 25', 'BANK'),
        ('ICICIBANK FUT 30 DEC 25', 'BANK'),
        ('AXISBANK FUT 30 DEC 25', 'BANK'),
        ('SBIN FUT 30 DEC 25', 'BANK'),
        ('KOTAKBANK FUT 30 DEC 25', 'BANK'),

        -- IT SECTOR
        ('INFY FUT 30 DEC 25', 'IT'),
        ('TCS FUT 30 DEC 25', 'IT'),
        ('WIPRO FUT 30 DEC 25', 'IT'),
        ('HCLTECH FUT 30 DEC 25', 'IT'),

        -- RIL SECTOR
        ('RELIANCE FUT 30 DEC 25', 'RIL')

    ) AS t(instrument_name, sector)
),

joined AS (
    SELECT 
        b.bucket_time,
        b.instrument_name,
        sm.sector,
        b.volume,
        b.buy_delta,
        b.sell_delta,
        b.net_delta,
        ROUND(b.net_delta / 100000.0, 3) AS net_delta_lakh
    FROM bucketed b
    LEFT JOIN sector_map sm 
        ON sm.instrument_name = b.instrument_name
)

SELECT 
    bucket_time AS ts_bucket,

    -- sector combined deltas in lakhs
    SUM(CASE WHEN sector = 'BANK' THEN net_delta_lakh ELSE 0 END) AS bank_cvd_lakh,
    SUM(CASE WHEN sector = 'IT' THEN net_delta_lakh ELSE 0 END) AS it_cvd_lakh,
    SUM(CASE WHEN sector = 'RIL' THEN net_delta_lakh ELSE 0 END) AS ril_cvd_lakh,

    -- full market CVD in lakhs
    SUM(net_delta_lakh) AS total_market_cvd_lakh

FROM joined
GROUP BY bucket_time
ORDER BY bucket_time;
";



                DataTable dt = new DataTable();
                using (var con = new NpgsqlConnection(_connectionString))
                {
                    await con.OpenAsync();
                    using (var cmd = new NpgsqlCommand(sql, con))
                    {
                        // pass array parameter for instrument_name = ANY(@instrs)
                        cmd.Parameters.AddWithValue("startUtc", startUtc);
                        cmd.Parameters.AddWithValue("endUtc", endUtc);
                        cmd.Parameters.AddWithValue("aggmin", aggmin);
                        // Postgres expects text[] type for arrays; Npgsql will infer
                        cmd.Parameters.AddWithValue("instrs", instrumentList.ToArray());

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            dt.Load(reader);
                        }
                    }
                }

                // Process the returned per-instrument buckets into sector aggregates + cumulative
                var finalList = ProcessSectorAggregation(dt, aggmin);
                dgSectorCVD.ItemsSource = finalList;
                MessageBox.Show($"Data loaded for {finalList.Count} time buckets");
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error: " + ex.Message);
            }
        }
        private readonly Dictionary<string, List<string>> _sectors = new Dictionary<string, List<string>>
        {
            { "BANK", new List<string> {
                "HDFCBANK FUT 30 DEC 25",
                "ICICIBANK FUT 30 DEC 25",
                "AXISBANK FUT 30 DEC 25",
                "SBIN FUT 30 DEC 25",
                "KOTAKBANK FUT 30 DEC 25"
            }},

            { "IT", new List<string> {
                "INFY FUT 30 DEC 25",
                "TCS FUT 30 DEC 25",
                "WIPRO FUT 30 DEC 25",
                "HCLTECH FUT 30 DEC 25"
            }},

            { "RIL", new List<string> {
                "RELIANCE FUT 30 DEC 25"
            }}
        };

        // Sector weights for final weighted score (change here to tune)
        private readonly Dictionary<string, double> _sectorWeights = new Dictionary<string, double>
        {
            { "BANK", 0.35 },
            { "IT", 0.13 },
            { "RIL", 0.10 }
        };

        // Model for DataGrid
        public class SectorCVDData
        {
            public DateTime BucketTime { get; set; }

            public double BankDelta { get; set; }
            public double ITDelta { get; set; }
            public double EnergyDelta { get; set; }

            public double BankCVD { get; set; }
            public double ITCVD { get; set; }
            public double EnergyCVD { get; set; }

            public double WeightedScore { get; set; }
            public string Signal { get; set; }
        }


        private List<SectorCVDData> ProcessSectorAggregation(DataTable dt, int aggmin)
        {
            var result = new List<SectorCVDData>();

            // cumulative tracking
            double cumBank = 0;
            double cumIT = 0;
            double cumEnergy = 0;

            // get all timestamps sorted
            var times = dt.AsEnumerable()
                          .Select(r => r.Field<DateTime>("ts_bucket"))
                          .Distinct()
                          .OrderBy(t => t)
                          .ToList();

            foreach (var t in times)
            {
                var row = dt.AsEnumerable().First(r => r.Field<DateTime>("ts_bucket") == t);

                // extract sector deltas (already in lakh from query)
                double bankDelta = row["bank_cvd_lakh"] == DBNull.Value ? 0 : Convert.ToDouble(row["bank_cvd_lakh"]);
                double itDelta = row["it_cvd_lakh"] == DBNull.Value ? 0 : Convert.ToDouble(row["it_cvd_lakh"]);
                double energyDelta = row["energy_cvd_lakh"] == DBNull.Value ? 0 : Convert.ToDouble(row["energy_cvd_lakh"]);

                // cumulative
                cumBank += bankDelta;
                cumIT += itDelta;
                cumEnergy += energyDelta;

                var item = new SectorCVDData
                {
                    BucketTime = t,

                    BankDelta = Math.Round(bankDelta, 2),
                    ITDelta = Math.Round(itDelta, 2),
                    EnergyDelta = Math.Round(energyDelta, 2),

                    BankCVD = Math.Round(cumBank, 2),
                    ITCVD = Math.Round(cumIT, 2),
                    EnergyCVD = Math.Round(cumEnergy, 2)
                };

                // scoring & signal
                item.WeightedScore = CalculateWeightedScoreForSectors(item);
                item.Signal = DetermineSignalForSectors(item);

                result.Add(item);
            }

            return result;
        }



        // Normalize and combine sector CVD into weighted score (0..100)
        private double CalculateWeightedScoreForSectors(SectorCVDData data)
        {
            // Use tanh normalization for stability and scale
            double normBank = NormalizeTanh(data.BankCVD);
            double normIt = NormalizeTanh(data.ITCVD);
            double normRil = NormalizeTanh(data.EnergyDelta);

            double score =
                (_sectorWeights.GetValueOrDefault("BANK", 0.0) * normBank) +
                (_sectorWeights.GetValueOrDefault("IT", 0.0) * normIt) +
                (_sectorWeights.GetValueOrDefault("RIL", 0.0) * normRil);

            // score is in -1..1 range (because tanh). Convert to 0..100
            double scaled = ((score + 1.0) / 2.0) * 100.0;
            return Math.Round(scaled, 2);
        }

        // simple tanh normalization with a scale factor (adjust scaleFactor to control sensitivity)
        private double NormalizeTanh(double x, double scaleFactor = 50000.0)
        {
            // Avoid overflow; x / scaleFactor roughly puts typical CVD into reasonable range
            return Math.Tanh(x / scaleFactor);
        }

        // Determine Buy/Sell signal based on sector cumulative signs & relative strength
        private string DetermineSignalForSectors(SectorCVDData d)
        {
            // Strong if both bank and it are aligned
            if (d.BankCVD > 0 && d.ITCVD > 0) return "Strong Buy";
            if (d.BankCVD < 0 && d.ITCVD < 0) return "Strong Sell";

            // If bank dominates but IT opposite
            if (d.BankCVD > 0 && d.ITCVD < 0) return "Bank Buy (Mixed)";
            if (d.BankCVD < 0 && d.ITCVD > 0) return "Bank Sell (Mixed)";

            // Fallback to RIL bias if both bank & it neutral
            if (Math.Abs(d.BankCVD) < 1e-9 && Math.Abs(d.ITCVD) < 1e-9)
            {
                if (d.EnergyDelta > 0) return "RIL Buy";
                if (d.EnergyDelta < 0) return "RIL Sell";
            }

            return "Neutral";
        }


        // small extension to fetch value or default


        private readonly Dictionary<string, double> _stockWeightages = new Dictionary<string, double>
{
    {"RELIANCE FUT 30 DEC 25", 10.3},    // Higher weight = more impact on Nifty
    {"INFY FUT 30 DEC 25", 3.1},
    {"HDFCBANK FUT 30 DEC 25", 7.3},
    {"TCS FUT 30 DEC 25", 5.4},
    {"ICICIBANK FUT 30 DEC 25", 7.1},

};


        private async void Nifty_Click(object sender, RoutedEventArgs e)
        {
            if (Timecom.SelectedItem == null)
            {
                MessageBox.Show("Please select Time Interval.");
                return;
            }

            var selectedDate = EndDatePicker.SelectedDate.Value;

            var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);
            var selectedTime = DateTime.ParseExact("15:30:00", "HH:mm:ss", CultureInfo.InvariantCulture);

            var startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                  firsttime.Hour, firsttime.Minute, firsttime.Second);

            var endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                  selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

            int aggmin = (int)Timecom.SelectedItem;
            var startUtc = startIST.ToUniversalTime();
            var endUtc = endIST.ToUniversalTime();

            var con = new NpgsqlConnection(_connectionString);
            await con.OpenAsync();


            string sql = @"
            WITH tick_data AS (
                SELECT 
                    instrument_name,
                    ts,
                    ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
                    price,
                    size,
                    LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) AS prev_price
                FROM raw_ticks
                WHERE ts BETWEEN @startUtc AND @endUtc
                  AND instrument_name IN (" + string.Join(",", _stockWeightages.Keys.Select(k => $"'{k}'")) + @")
            ),

            calculated_deltas AS (
                SELECT 
                    instrument_name,
                    ts,
                    ts_ist,
                    size,
                    CASE 
                        WHEN price > prev_price THEN size
                        ELSE 0 
                    END AS tick_buy,
                    CASE 
                        WHEN price < prev_price THEN size
                        ELSE 0 
                    END AS tick_sell
                FROM tick_data
                WHERE prev_price IS NOT NULL
            ),

            bucketed_data AS (
                SELECT 
                    instrument_name,
                    (date_trunc('minute', ts_ist) 
                     - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS bucket_time,
                    SUM(size) AS volume,
                    SUM(tick_buy) AS buy_delta,
                    SUM(tick_sell) AS sell_delta
                FROM calculated_deltas
                GROUP BY instrument_name, bucket_time
            ),

            aggregated_data AS (
                SELECT 
                    bucket_time,
                    SUM(volume) AS total_volume,
                    SUM(buy_delta) AS total_buy_delta,
                    SUM(sell_delta) AS total_sell_delta,
                    (SUM(buy_delta) - SUM(sell_delta)) AS net_delta
                FROM bucketed_data
                GROUP BY bucket_time
            ),

            cumulative_data AS (
                SELECT 
                    bucket_time,
                    total_volume,
                    total_buy_delta,
                    total_sell_delta,
                    net_delta,
                    SUM(net_delta) OVER (ORDER BY bucket_time) AS cumulative_delta
                FROM aggregated_data
            )

            SELECT 
                bucket_time AS BlockTime,
                total_volume / 100000.0 AS VolumeLakh,
                net_delta / 100000.0 AS DeltaLakh,
                cumulative_delta / 100000.0 AS CumCVDLakh,
                total_buy_delta / 100000.0 AS BuyLakh,
                total_sell_delta / 100000.0 AS SellLakh
            FROM cumulative_data
            ORDER BY bucket_time ASC;";

            var cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddWithValue("startUtc", startUtc);
            cmd.Parameters.AddWithValue("endUtc", endUtc);
            cmd.Parameters.AddWithValue("aggmin", aggmin);


            //var cmd = new NpgsqlCommand(sql, con);
            //cmd.Parameters.AddWithValue("startIst", startIST);  // Use IST directly
            //cmd.Parameters.AddWithValue("endIst", endIST);      // Use IST directly
            //cmd.Parameters.AddWithValue("aggmin", aggmin);

            var dt = new DataTable();
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                dt.Load(reader);
            }

            // Calculate weighted scores and create final data
            var finalData = CalculateWeightedScores(dt);
            DataGrid1.ItemsSource = finalData;
            MessageBox.Show($"Data loaded for {finalData.Count} time buckets");
        }

        private List<StockAggregateData> CalculateWeightedScores(DataTable dt)
        {
            var result = new List<StockAggregateData>();

            // Track min/max values for normalization
            double maxVolume = 0, maxDelta = 0, maxCumCVD = 0;
            double minVolume = double.MaxValue, minDelta = double.MaxValue, minCumCVD = double.MaxValue;

            // First pass: find min/max values for normalization
            foreach (DataRow row in dt.Rows)
            {
                var volumeLakh = Math.Abs(Convert.ToDouble(row["VolumeLakh"]));
                var deltaLakh = Convert.ToDouble(row["DeltaLakh"]);
                var cumCVDLakh = Convert.ToDouble(row["CumCVDLakh"]);

                maxVolume = Math.Max(maxVolume, volumeLakh);
                maxDelta = Math.Max(maxDelta, Math.Abs(deltaLakh));
                maxCumCVD = Math.Max(maxCumCVD, Math.Abs(cumCVDLakh));

                minVolume = Math.Min(minVolume, volumeLakh);
                minDelta = Math.Min(minDelta, Math.Abs(deltaLakh));
                minCumCVD = Math.Min(minCumCVD, Math.Abs(cumCVDLakh));
            }

            // Ensure we don't divide by zero
            maxVolume = Math.Max(maxVolume, 1);
            maxDelta = Math.Max(maxDelta, 1);
            maxCumCVD = Math.Max(maxCumCVD, 1);

            // Second pass: calculate scores
            foreach (DataRow row in dt.Rows)
            {
                var blockTime = Convert.ToDateTime(row["BlockTime"]);
                var volumeLakh = Convert.ToDouble(row["VolumeLakh"]);
                var deltaLakh = Convert.ToDouble(row["DeltaLakh"]);
                var cumCVDLakh = Convert.ToDouble(row["CumCVDLakh"]);
                var buyLakh = Convert.ToDouble(row["BuyLakh"]);
                var sellLakh = Convert.ToDouble(row["SellLakh"]);

                // Calculate weighted score (0-100)
                double weightedScore = CalculateWeightedScore(deltaLakh, cumCVDLakh, volumeLakh,
                                                             maxDelta, maxCumCVD, maxVolume);

                string signal = DetermineSignal(deltaLakh, cumCVDLakh);
                string alert = GenerateAlert(deltaLakh, cumCVDLakh, volumeLakh);

                result.Add(new StockAggregateData
                {
                    BlockTime = blockTime,
                    VolumeLakh = Math.Round(volumeLakh, 2),
                    DeltaLakh = Math.Round(deltaLakh, 2),
                    CumCVDLakh = Math.Round(cumCVDLakh, 2),
                    WeightedScore = weightedScore,
                    Signal = signal,
                    Alert = alert
                });
            }

            return result;
        }

        private double CalculateWeightedScore(double deltaLakh, double cumCVDLakh, double volumeLakh,
                                            double maxDelta, double maxCumCVD, double maxVolume)
        {
            // Calculate base scores (0-100 range for each component)

            // Delta Score: Normalize between -maxDelta to +maxDelta to 0-100
            // Positive delta = higher score, Negative delta = lower score
            double deltaScore = NormalizeTo100(deltaLakh, -maxDelta, maxDelta);

            // CVD Score: Normalize between -maxCumCVD to +maxCumCVD to 0-100
            double cvdScore = NormalizeTo100(cumCVDLakh, -maxCumCVD, maxCumCVD);

            // Volume Score: Normalize between 0 to maxVolume to 0-100
            double volumeScore = NormalizeTo100(Math.Abs(volumeLakh), 0, maxVolume);

            // Calculate total weightage from all stocks
            double totalWeightage = _stockWeightages.Values.Sum();

            // Calculate average weightage impact (normalize to 0-1 range)
            double avgWeightage = totalWeightage / (_stockWeightages.Count * 10.0); // Normalize for scoring

            // Weighted composite score (0-100)
            // Delta (40%), CVD (30%), Volume (30%)
            double compositeScore = (deltaScore * 0.4) + (cvdScore * 0.3) + (volumeScore * 0.3);

            // Apply stock weightage multiplier 
            // Higher average weight = higher score impact (0.7 to 1.3 multiplier)
            double weightageMultiplier = 0.7 + (avgWeightage * 0.6);

            double finalScore = compositeScore * weightageMultiplier;

            // Ensure score stays within 0-100 range
            return Math.Max(0, Math.Min(100, Math.Round(finalScore, 2)));
        }

        private double NormalizeTo100(double value, double min, double max)
        {
            if (max - min == 0) return 50; // Neutral score if no range

            double normalized = ((value - min) / (max - min)) * 100;
            return Math.Max(0, Math.Min(100, normalized));
        }

        private string DetermineSignal(double deltaLakh, double cumCVDLakh)
        {
            // More sensitive thresholds for better signal detection
            if (deltaLakh > 25 && cumCVDLakh > 50)
                return "Strong Bullish";
            else if (deltaLakh > 10)
                return "Bullish";
            else if (deltaLakh < -25 && cumCVDLakh < -50)
                return "Strong Bearish";
            else if (deltaLakh < -10)
                return "Bearish";
            else if (Math.Abs(deltaLakh) < 5)
                return "Neutral";
            else
                return "Mixed";
        }

        private string GenerateAlert(double deltaLakh, double cumCVDLakh, double volumeLakh)
        {
            var alerts = new List<string>();

            if (deltaLakh > 50) alerts.Add("Heavy Buying");
            else if (deltaLakh < -50) alerts.Add("Heavy Selling");

            if (cumCVDLakh > 200) alerts.Add("Strong Bullish Trend");
            else if (cumCVDLakh < -200) alerts.Add("Strong Bearish Trend");

            if (volumeLakh > 500) alerts.Add("High Volume Activity");

            return alerts.Count > 0 ? string.Join(", ", alerts) : "Normal";
        }

        // Data class for binding
        public class StockAggregateData
        {
            public DateTime BlockTime { get; set; }
            public double VolumeLakh { get; set; }
            public double DeltaLakh { get; set; }
            public double CumCVDLakh { get; set; }
            public double WeightedScore { get; set; }
            public string Signal { get; set; }
            public string Alert { get; set; }
        }

        private async void Button_Click(object sender, RoutedEventArgs e)
        {
            //InstrumentCombo.ItemsSource = null;
            InstrumentCombo.ItemsSource = null;
            InstrumentCombo.Items.Clear();   // Important
            //MyComboBox.ItemsSource = values;

            if (EndDatePicker.SelectedDate == null)
            {
                MessageBox.Show("Please select End Date.");
                return;
            }
            var selectedDate = EndDatePicker.SelectedDate.Value;
            selectedDate = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day);
            //selectedDate = selectedDate.ToString(par);
            try
            {
                string query = @"
                    SELECT DISTINCT instrument_name
                    FROM public.raw_ticks
                    WHERE ts::date = @date
                    ORDER BY instrument_name;
                ";

                List<string> values = new List<string>();

                using (var conn = new NpgsqlConnection(_connectionString))
                {
                    conn.Open();

                    using (var cmd = new NpgsqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@date", selectedDate.Date);

                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                values.Add(reader.GetString(0));
                            }
                        }
                    }
                }

                InstrumentCombo.ItemsSource = values;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, "Database Error");
            }
            MessageBox.Show("Done");
            await Task.CompletedTask;
        }
        //ScanDeltaSpikes_Click
        private async void ScanDeltaSpikes_Click(object sender, RoutedEventArgs e)
        {
            if (Timecom.SelectedItem == null)
            {
                MessageBox.Show("Please select Time Interval.");
                return;
            }
            if (EndDatePicker.SelectedDate == null)
            {
                MessageBox.Show("Please select End Date.");
                return;
            }

            var selectedDate = EndDatePicker.SelectedDate.Value;
            //var selectedTime = DateTime.ParseExact(EndTimeComboBox.SelectedItem.ToString(), "HH:mm:ss", CultureInfo.InvariantCulture);
            var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);
            var selectedTime = DateTime.ParseExact("15:30:00", "HH:mm:ss", CultureInfo.InvariantCulture);
            var startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                  firsttime.Hour, firsttime.Minute, firsttime.Second);
            var endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                  selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

            int aggmin = (int)Timecom.SelectedItem;
            var startUtc = startIST.ToUniversalTime();
            var endUtc = endIST.ToUniversalTime();

            var con = new NpgsqlConnection(_connectionString);
            await con.OpenAsync();

            // FIXED QUERY - No window functions inside aggregates
            string testQuery = @"
-- STEP 1: First calculate tick-by-tick delta
WITH tick_delta AS (
    SELECT 
        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        instrument_name,
        price,
        size,
        CASE 
            WHEN price > LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
            ELSE 0 
        END AS buy_size,
        CASE 
            WHEN price < LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
            ELSE 0 
        END AS sell_size
    FROM raw_ticks
    WHERE ts BETWEEN @startUtc AND @endUtc
      AND instrument_name IN (
          SELECT DISTINCT instrument_name 
          FROM raw_ticks 
          WHERE ts BETWEEN @startUtc AND @endUtc 
          ORDER BY instrument_name
          LIMIT 5  -- Test with only 5 stocks
      )
),

-- STEP 2: Now aggregate by time interval
aggregated_delta AS (
    SELECT 
        instrument_name,
        (date_trunc('minute', ts_ist) 
            - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS candle_time,
        MAX(price) AS high_price,
        MIN(price) AS low_price,
        AVG(price) AS avg_price,
        SUM(size) AS total_volume,
        SUM(buy_size) AS buy_volume,
        SUM(sell_size) AS sell_volume,
        (SUM(buy_size) - SUM(sell_size)) AS net_delta,
        (SUM(buy_size) - SUM(sell_size)) / 100000.0 AS net_delta_lakh
    FROM tick_delta
    GROUP BY instrument_name, 
            (date_trunc('minute', ts_ist) 
             - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin)))
)

-- STEP 3: Show results
SELECT 
    instrument_name,
    candle_time,
    avg_price,
    high_price,
    low_price,
    total_volume,
    buy_volume,
    sell_volume,
    net_delta,
    net_delta_lakh,
    CASE 
        WHEN net_delta_lakh > 0 THEN 'BUYING'
        WHEN net_delta_lakh < 0 THEN 'SELLING'
        ELSE 'NEUTRAL'
    END AS direction
FROM aggregated_delta
WHERE ABS(net_delta_lakh) > 0.5
ORDER BY instrument_name, candle_time
LIMIT 50;
";

            try
            {
                var cmd = new NpgsqlCommand(testQuery, con);
                cmd.Parameters.AddWithValue("startUtc", startUtc);
                cmd.Parameters.AddWithValue("endUtc", endUtc);
                cmd.Parameters.AddWithValue("aggmin", aggmin);

                cmd.CommandTimeout = 60;

                var dataTable = new DataTable();
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    dataTable.Load(reader);
                }

                if (dataTable.Rows.Count > 0)
                {
                    MessageBox.Show($"SUCCESS! Found {dataTable.Rows.Count} delta entries.\n" +
                                  $"First stock: {dataTable.Rows[0]["instrument_name"]}\n" +
                                  $"Sample delta: {dataTable.Rows[0]["net_delta_lakh"]} lakh");

                    // Now run the cup pattern query
                    await RunCupPatternQuery(con, startUtc, endUtc, aggmin);
                }
                else
                {
                    MessageBox.Show("No delta data found. Try:\n" +
                                  "1. Check if selected date is a trading day\n" +
                                  "2. Time range should be between 9:15 AM to 3:30 PM\n" +
                                  "3. Database might not have data for selected period");
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error in test query: {ex.Message}");
            }
            finally
            {
                con.Close();
            }
        }

        private async Task RunCupPatternQuery(NpgsqlConnection con, DateTime startUtc, DateTime endUtc, int aggmin)
        {
            string cupPatternQuery = @"
-- STEP 1: Calculate tick delta first
WITH tick_data AS (
    SELECT 
        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        instrument_name,
        price,
        size,
        CASE 
            WHEN price > LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
            ELSE 0 
        END AS buy_size,
        CASE 
            WHEN price < LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
            ELSE 0 
        END AS sell_size
    FROM raw_ticks
    WHERE ts BETWEEN @startUtc AND @endUtc
),

-- STEP 2: Aggregate by time interval
candle_data AS (
    SELECT 
        instrument_name,
        (date_trunc('minute', ts_ist) 
            - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS candle_time,
        MAX(price) AS high_price,
        MIN(price) AS low_price,
        AVG(price) AS avg_price,
        SUM(buy_size) AS buy_volume,
        SUM(sell_size) AS sell_volume,
        (SUM(buy_size) - SUM(sell_size)) AS net_delta,
        (SUM(buy_size) - SUM(sell_size)) / 100000.0 AS net_delta_lakh,
        SUM(size) AS total_volume
    FROM tick_data
    GROUP BY instrument_name, 
            (date_trunc('minute', ts_ist) 
             - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin)))
),

-- STEP 3: Calculate rolling statistics for each stock
rolling_stats AS (
    SELECT 
        *,
        -- Calculate rolling average delta (last 20 periods)
        AVG(ABS(net_delta_lakh)) OVER (
            PARTITION BY instrument_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_delta,
        
        -- Calculate rolling standard deviation (last 20 periods)
        STDDEV(ABS(net_delta_lakh)) OVER (
            PARTITION BY instrument_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS rolling_stddev_delta,
        
        -- Calculate rolling average volume
        AVG(total_volume) OVER (
            PARTITION BY instrument_name 
            ORDER BY candle_time 
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_volume,
        
        ROW_NUMBER() OVER (PARTITION BY instrument_name ORDER BY candle_time) AS candle_num
    FROM candle_data
),

-- STEP 4: Identify spikes dynamically
spikes AS (
    SELECT 
        instrument_name,
        candle_time,
        avg_price,
        high_price,
        low_price,
        net_delta_lakh,
        total_volume,
        rolling_avg_delta,
        rolling_stddev_delta,
        rolling_avg_volume,
        
        -- Calculate z-score for delta (how many standard deviations from mean)
        CASE 
            WHEN rolling_stddev_delta > 0 
            THEN ABS(net_delta_lakh - rolling_avg_delta) / rolling_stddev_delta
            ELSE 0 
        END AS delta_zscore,
        
        -- Calculate volume ratio (current volume vs average)
        CASE 
            WHEN rolling_avg_volume > 0 
            THEN total_volume / rolling_avg_volume
            ELSE 1 
        END AS volume_ratio,
        
        -- Dynamic spike detection flags
        CASE 
            -- Condition 1: Z-score > 2 (statistically significant)
            WHEN ABS(net_delta_lakh - rolling_avg_delta) / NULLIF(rolling_stddev_delta, 0) > 2
            -- Condition 2: Absolute delta is at least 0.5 lakh (minimum threshold)
            AND ABS(net_delta_lakh) >= 0.5
            -- Condition 3: Volume is above average (confirmation)
            AND total_volume >= rolling_avg_volume * 0.8
            THEN TRUE
            -- Alternative: Very high absolute delta (> 5 lakh) regardless of stats
            WHEN ABS(net_delta_lakh) >= 5.0
            THEN TRUE
            ELSE FALSE
        END AS is_spike,
        
        -- Spike strength score (0-10)
        CASE 
            WHEN ABS(net_delta_lakh - rolling_avg_delta) / NULLIF(rolling_stddev_delta, 0) > 3 THEN 10
            WHEN ABS(net_delta_lakh - rolling_avg_delta) / NULLIF(rolling_stddev_delta, 0) > 2.5 THEN 8
            WHEN ABS(net_delta_lakh - rolling_avg_delta) / NULLIF(rolling_stddev_delta, 0) > 2 THEN 6
            WHEN ABS(net_delta_lakh) >= 3.0 THEN 5
            WHEN ABS(net_delta_lakh) >= 2.0 THEN 4
            WHEN ABS(net_delta_lakh) >= 1.0 THEN 3
            ELSE 0
        END AS spike_strength,
        
        ROW_NUMBER() OVER (PARTITION BY instrument_name ORDER BY candle_time) AS row_num
        
    FROM rolling_stats
    WHERE candle_num > 20  -- Need at least 20 periods for statistics
),

-- STEP 5: Filter only spikes
filtered_spikes AS (
    SELECT 
        instrument_name,
        candle_time,
        avg_price,
        high_price,
        low_price,
        net_delta_lakh,
        delta_zscore,
        volume_ratio,
        spike_strength
    FROM spikes
    WHERE is_spike = TRUE
),

-- STEP 6: Find pairs of spikes for same stock
spike_pairs AS (
    SELECT 
        s1.instrument_name,
        s1.candle_time AS time_1,
        s1.avg_price AS price_1,
        s1.net_delta_lakh AS delta_1,
        s1.spike_strength AS strength_1,
        s1.high_price AS high_1,
        s1.low_price AS low_1,
        s1.delta_zscore AS zscore_1,
        
        s2.candle_time AS time_2,
        s2.avg_price AS price_2,
        s2.net_delta_lakh AS delta_2,
        s2.spike_strength AS strength_2,
        s2.delta_zscore AS zscore_2
    FROM filtered_spikes s1
    JOIN filtered_spikes s2 
        ON s1.instrument_name = s2.instrument_name
        AND s2.candle_time > s1.candle_time
        AND s2.candle_time <= s1.candle_time + INTERVAL '120 minutes'
        -- Second spike has higher absolute delta OR higher z-score
        AND (ABS(s2.net_delta_lakh) > ABS(s1.net_delta_lakh) OR s2.delta_zscore > s1.delta_zscore)
        -- Same direction
        AND s1.net_delta_lakh * s2.net_delta_lakh > 0
),

-- STEP 7: Find lowest price between spikes
spikes_with_trough AS (
    SELECT 
        sp.*,
        -- Find trough price between spikes
        (
            SELECT MIN(low_price)
            FROM candle_data cd
            WHERE cd.instrument_name = sp.instrument_name
              AND cd.candle_time > sp.time_1
              AND cd.candle_time < sp.time_2
        ) AS trough_price,
        
        -- Find average price between spikes
        (
            SELECT AVG(avg_price)
            FROM candle_data cd
            WHERE cd.instrument_name = sp.instrument_name
              AND cd.candle_time > sp.time_1
              AND cd.candle_time < sp.time_2
        ) AS avg_price_between
    FROM spike_pairs sp
),

-- STEP 8: Calculate metrics
calculated_metrics AS (
    SELECT 
        *,
        -- Price difference percentage
        ABS(price_1 - price_2) * 100.0 / price_1 AS price_diff_pct,
        
        -- Time gap in minutes
        EXTRACT(EPOCH FROM (time_2 - time_1)) / 60 AS minutes_gap,
        
        -- Correction percentage (if trough exists)
        CASE 
            WHEN trough_price IS NOT NULL 
            THEN (price_1 - trough_price) * 100.0 / price_1
            ELSE 0 
        END AS correction_pct,
        
        -- Delta increase percentage
        CASE 
            WHEN delta_1 != 0 
            THEN ((delta_2 - delta_1) / ABS(delta_1)) * 100
            ELSE 0 
        END AS delta_inc_pct,
        
        -- Strength increase
        strength_2 - strength_1 AS strength_increase,
        
        -- Z-score increase
        zscore_2 - zscore_1 AS zscore_increase
    FROM spikes_with_trough
    WHERE trough_price IS NOT NULL
),

-- STEP 9: Validate patterns with dynamic thresholds
valid_patterns AS (
    SELECT 
        instrument_name,
        time_1,
        price_1,
        delta_1,
        strength_1,
        zscore_1,
        trough_price,
        correction_pct,
        time_2,
        price_2,
        delta_2,
        strength_2,
        zscore_2,
        price_diff_pct,
        minutes_gap,
        delta_inc_pct,
        strength_increase,
        zscore_increase,
        
        -- Dynamic validation based on spike strength
        CASE 
            -- For strong spikes (strength >= 6), allow wider price match
            WHEN strength_1 >= 6 AND strength_2 >= 6 
            THEN price_diff_pct <= 0.75  -- 0.75% for strong spikes
            ELSE price_diff_pct <= 0.5   -- 0.5% for regular spikes
        END AS price_match_ok,
        
        -- Time gap varies based on spike strength
        CASE 
            WHEN strength_1 >= 8 AND strength_2 >= 8
            THEN minutes_gap BETWEEN 10 AND 180  -- 10 min to 3 hours for very strong spikes
            ELSE minutes_gap BETWEEN 15 AND 120  -- 15 min to 2 hours for regular spikes
        END AS time_gap_ok,
        
        -- Correction tolerance based on spike strength
        CASE 
            WHEN strength_1 >= 7 AND strength_2 >= 7
            THEN correction_pct BETWEEN 0.5 AND 8.0  -- Allow deeper corrections for strong spikes
            ELSE correction_pct BETWEEN 0.3 AND 5.0  -- Normal range
        END AS correction_ok,
        
        -- Delta increase requirement varies
        CASE 
            WHEN strength_1 >= 5  -- Already strong first spike
            THEN delta_2 > delta_1 * 0.8  -- Second spike should be at least 80% of first
            ELSE delta_2 > delta_1  -- Regular requirement
        END AS delta_increased,
        
        -- Pattern type
        CASE 
            WHEN delta_1 > 0 THEN 'BULLISH'
            ELSE 'BEARISH'
        END AS pattern_type,
        
        -- Overall pattern confidence (0-10)
        CASE 
            WHEN strength_1 >= 8 AND strength_2 >= 8 AND delta_inc_pct >= 50 THEN 10
            WHEN strength_1 >= 6 AND strength_2 >= 6 AND delta_inc_pct >= 30 THEN 8
            WHEN strength_1 >= 4 AND strength_2 >= 4 AND delta_inc_pct >= 20 THEN 6
            WHEN delta_inc_pct >= 10 THEN 4
            ELSE 2
        END AS pattern_confidence
        
    FROM calculated_metrics
)

-- STEP 10: Final results
SELECT 
    instrument_name AS stock,
    time_1 AS first_time,
    ROUND(price_1::numeric, 2) AS first_price,
    ROUND(delta_1::numeric, 2) AS first_delta_lakh,
    strength_1 AS first_strength,
    ROUND(zscore_1::numeric, 1) AS first_zscore,
    
    ROUND(trough_price::numeric, 2) AS trough_price,
    ROUND(correction_pct::numeric, 2) AS correction_percent,
    
    time_2 AS second_time,
    ROUND(price_2::numeric, 2) AS second_price,
    ROUND(delta_2::numeric, 2) AS second_delta_lakh,
    strength_2 AS second_strength,
    ROUND(zscore_2::numeric, 1) AS second_zscore,
    
    ROUND(price_diff_pct::numeric, 2) AS price_match_percent,
    ROUND(minutes_gap::numeric, 0) AS time_gap_minutes,
    ROUND(delta_inc_pct::numeric, 1) AS delta_increase_percent,
    
    pattern_type,
    pattern_confidence,
    
    CASE 
        WHEN strength_increase > 0 THEN 'INCREASING'
        WHEN strength_increase = 0 THEN 'STABLE'
        ELSE 'DECREASING'
    END AS strength_trend,
    
    -- Dynamic pattern status
    CASE 
        WHEN price_match_ok AND time_gap_ok AND correction_ok AND delta_increased
        THEN CASE 
            WHEN pattern_confidence >= 8 THEN 'STRONG_VALID'
            WHEN pattern_confidence >= 6 THEN 'MODERATE_VALID'
            ELSE 'WEAK_VALID'
        END
        ELSE 'INVALID'
    END AS pattern_status,
    
    -- Dynamic entry suggestion based on confidence
    CASE 
        WHEN price_match_ok AND time_gap_ok AND correction_ok AND delta_increased AND pattern_type = 'BULLISH'
        THEN CASE 
            WHEN pattern_confidence >= 8 THEN 'STRONG BUY near ' || ROUND(trough_price::numeric * 1.005, 2)
            WHEN pattern_confidence >= 6 THEN 'BUY near ' || ROUND(trough_price::numeric * 1.01, 2)
            ELSE 'CONSIDER BUY near ' || ROUND(trough_price::numeric * 1.015, 2)
        END
        WHEN price_match_ok AND time_gap_ok AND correction_ok AND delta_increased AND pattern_type = 'BEARISH'
        THEN CASE 
            WHEN pattern_confidence >= 8 THEN 'STRONG SELL near ' || ROUND(trough_price::numeric * 0.995, 2)
            WHEN pattern_confidence >= 6 THEN 'SELL near ' || ROUND(trough_price::numeric * 0.99, 2)
            ELSE 'CONSIDER SELL near ' || ROUND(trough_price::numeric * 0.985, 2)
        END
        ELSE 'NO ENTRY'
    END AS suggestion
    
FROM valid_patterns
WHERE price_match_ok AND time_gap_ok AND correction_ok AND delta_increased
ORDER BY pattern_confidence DESC, delta_inc_pct DESC
LIMIT 100;
";
            // SIMPLIFIED CUP PATTERN QUERY
            //            string cupPatternQuery = @"
            //-- STEP 1: Calculate tick delta first
            //WITH tick_data AS (
            //    SELECT 
            //        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
            //        instrument_name,
            //        price,
            //        size,
            //        CASE 
            //            WHEN price > LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
            //            ELSE 0 
            //        END AS buy_size,
            //        CASE 
            //            WHEN price < LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
            //            ELSE 0 
            //        END AS sell_size
            //    FROM raw_ticks
            //    WHERE ts BETWEEN @startUtc AND @endUtc
            //),

            //-- STEP 2: Aggregate by time interval
            //candle_data AS (
            //    SELECT 
            //        instrument_name,
            //        (date_trunc('minute', ts_ist) 
            //            - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS candle_time,
            //        MAX(price) AS high_price,
            //        MIN(price) AS low_price,
            //        AVG(price) AS avg_price,
            //        SUM(buy_size) AS buy_volume,
            //        SUM(sell_size) AS sell_volume,
            //        (SUM(buy_size) - SUM(sell_size)) AS net_delta,
            //        (SUM(buy_size) - SUM(sell_size)) / 100000.0 AS net_delta_lakh
            //    FROM tick_data
            //    GROUP BY instrument_name, 
            //            (date_trunc('minute', ts_ist) 
            //             - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin)))
            //),

            //-- STEP 3: Find spikes (delta >= 1 lakh)
            //spikes AS (
            //    SELECT 
            //        instrument_name,
            //        candle_time,
            //        avg_price,
            //        high_price,
            //        low_price,
            //        net_delta_lakh,
            //        ROW_NUMBER() OVER (PARTITION BY instrument_name ORDER BY candle_time) AS row_num
            //    FROM candle_data
            //    WHERE ABS(net_delta_lakh) >= 1.0  -- Minimum 1 lakh delta
            //),

            //-- STEP 4: Find pairs of spikes for same stock
            //spike_pairs AS (
            //    SELECT 
            //        s1.instrument_name,
            //        s1.candle_time AS time_1,
            //        s1.avg_price AS price_1,
            //        s1.net_delta_lakh AS delta_1,
            //        s1.high_price AS high_1,
            //        s1.low_price AS low_1,

            //        s2.candle_time AS time_2,
            //        s2.avg_price AS price_2,
            //        s2.net_delta_lakh AS delta_2
            //    FROM spikes s1
            //    JOIN spikes s2 
            //        ON s1.instrument_name = s2.instrument_name
            //        AND s2.row_num > s1.row_num  -- Second spike after first
            //        AND s2.candle_time <= s1.candle_time + INTERVAL '120 minutes'
            //        -- Second spike has higher absolute delta
            //        AND ABS(s2.net_delta_lakh) > ABS(s1.net_delta_lakh)
            //        -- Same direction
            //        AND s1.net_delta_lakh * s2.net_delta_lakh > 0
            //),

            //-- STEP 5: Find lowest price between spikes
            //spikes_with_trough AS (
            //    SELECT 
            //        sp.*,
            //        -- Find trough price between spikes
            //        (
            //            SELECT MIN(low_price)
            //            FROM candle_data cd
            //            WHERE cd.instrument_name = sp.instrument_name
            //              AND cd.candle_time > sp.time_1
            //              AND cd.candle_time < sp.time_2
            //        ) AS trough_price
            //    FROM spike_pairs sp
            //),

            //-- STEP 6: Calculate metrics
            //calculated_metrics AS (
            //    SELECT 
            //        *,
            //        -- Price difference percentage
            //        ABS(price_1 - price_2) * 100.0 / price_1 AS price_diff_pct,

            //        -- Time gap in minutes
            //        EXTRACT(EPOCH FROM (time_2 - time_1)) / 60 AS minutes_gap,

            //        -- Correction percentage (if trough exists)
            //        CASE 
            //            WHEN trough_price IS NOT NULL 
            //            THEN (price_1 - trough_price) * 100.0 / price_1
            //            ELSE 0 
            //        END AS correction_pct,

            //        -- Delta increase percentage
            //        CASE 
            //            WHEN delta_1 != 0 
            //            THEN ((delta_2 - delta_1) / ABS(delta_1)) * 100
            //            ELSE 0 
            //        END AS delta_inc_pct
            //    FROM spikes_with_trough
            //    WHERE trough_price IS NOT NULL
            //),

            //-- STEP 7: Validate patterns
            //valid_patterns AS (
            //    SELECT 
            //        instrument_name,
            //        time_1,
            //        price_1,
            //        delta_1,
            //        trough_price,
            //        correction_pct,
            //        time_2,
            //        price_2,
            //        delta_2,
            //        price_diff_pct,
            //        minutes_gap,
            //        delta_inc_pct,

            //        -- Validate criteria
            //        price_diff_pct <= 0.5 AS price_match_ok,
            //        minutes_gap BETWEEN 15 AND 120 AS time_gap_ok,
            //        correction_pct BETWEEN 0.3 AND 5.0 AS correction_ok,
            //        delta_2 > delta_1 AS delta_increased,

            //        -- Pattern type
            //        CASE 
            //            WHEN delta_1 > 0 THEN 'BULLISH'
            //            ELSE 'BEARISH'
            //        END AS pattern_type
            //    FROM calculated_metrics
            //)

            //-- STEP 8: Final results
            //SELECT 
            //    instrument_name AS stock,
            //    time_1 AS first_time,
            //    ROUND(price_1::numeric, 2) AS first_price,
            //    ROUND(delta_1::numeric, 2) AS first_delta_lakh,

            //    ROUND(trough_price::numeric, 2) AS trough_price,
            //    ROUND(correction_pct::numeric, 2) AS correction_percent,

            //    time_2 AS second_time,
            //    ROUND(price_2::numeric, 2) AS second_price,
            //    ROUND(delta_2::numeric, 2) AS second_delta_lakh,

            //    ROUND(price_diff_pct::numeric, 2) AS price_match_percent,
            //    ROUND(minutes_gap::numeric, 0) AS time_gap_minutes,
            //    ROUND(delta_inc_pct::numeric, 1) AS delta_increase_percent,

            //    pattern_type,

            //    CASE 
            //        WHEN price_match_ok AND time_gap_ok AND correction_ok AND delta_increased
            //        THEN 'VALID'
            //        ELSE 'INVALID'
            //    END AS pattern_status,

            //    CASE 
            //        WHEN price_match_ok AND time_gap_ok AND correction_ok AND delta_increased AND pattern_type = 'BULLISH'
            //        THEN 'Buy near ' || ROUND(trough_price::numeric * 1.01, 2)
            //        WHEN price_match_ok AND time_gap_ok AND correction_ok AND delta_increased AND pattern_type = 'BEARISH'
            //        THEN 'Sell near ' || ROUND(trough_price::numeric * 0.99, 2)
            //        ELSE 'No Entry'
            //    END AS suggestion

            //FROM valid_patterns
            //WHERE price_match_ok AND time_gap_ok AND correction_ok AND delta_increased
            //ORDER BY delta_inc_pct DESC
            //LIMIT 50;
            //";

            try
            {
                var cmd = new NpgsqlCommand(cupPatternQuery, con);
                cmd.Parameters.AddWithValue("startUtc", startUtc);
                cmd.Parameters.AddWithValue("endUtc", endUtc);
                cmd.Parameters.AddWithValue("aggmin", aggmin);

                cmd.CommandTimeout = 120;

                var dataTable = new DataTable();
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    dataTable.Load(reader);
                }

                if (dataTable.Rows.Count > 0)
                {
                    MessageBox.Show($"FOUND {dataTable.Rows.Count} CUP PATTERNS!");

                    // Show sample of first 3 patterns
                    var sampleInfo = new StringBuilder("Sample Patterns:\n");
                    for (int i = 0; i < Math.Min(3, dataTable.Rows.Count); i++)
                    {
                        var row = dataTable.Rows[i];
                        sampleInfo.AppendLine($"{row["stock"]} ({row["pattern_type"]}):");
                        sampleInfo.AppendLine($"  First: {row["first_time"]:HH:mm} @ ₹{row["first_price"]}, Δ: {row["first_delta_lakh"]}L");
                        sampleInfo.AppendLine($"  Trough: ₹{row["trough_price"]} ({row["correction_percent"]}% correction)");
                        sampleInfo.AppendLine($"  Second: {row["second_time"]:HH:mm} @ ₹{row["second_price"]}, Δ: {row["second_delta_lakh"]}L");
                        sampleInfo.AppendLine($"  Δ Increase: {row["delta_increase_percent"]}%");
                        sampleInfo.AppendLine($"  Suggestion: {row["suggestion"]}");
                        sampleInfo.AppendLine();
                    }
                    MessageBox.Show(sampleInfo.ToString());

                    // Bind to DataGrid
                    BindToDataGrid(dataTable);
                }
                else
                {
                    MessageBox.Show("No cup patterns found. This could mean:\n" +
                                  "1. No stocks matched the pattern criteria\n" +
                                  "2. Try using 5-minute intervals\n" +
                                  "3. Check during volatile market periods\n" +
                                  "4. The pattern is rare - might not occur every day");
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error in cup pattern query: {ex.Message}");
            }
        }

        private void BindToDataGrid(DataTable dataTable)
        {
            // Create DataGrid if it doesn't exist
            if (CupPatternGrid == null)
            {
                CupPatternGrid = new DataGrid();
                CupPatternGrid.Margin = new Thickness(10);
                CupPatternGrid.AutoGenerateColumns = false;
                CupPatternGrid.IsReadOnly = true;

                // You might need to add this to your window's grid
                // Example: MainGrid.Children.Add(CupPatternGrid);
            }

            // Clear existing columns
            CupPatternGrid.Columns.Clear();

            // Add columns
            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Stock",
                Binding = new Binding("stock"),
                Width = 80
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Type",
                Binding = new Binding("pattern_type"),
                Width = 70
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Status",
                Binding = new Binding("pattern_status"),
                Width = 70
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "First Time",
                Binding = new Binding("first_time") { StringFormat = "HH:mm" },
                Width = 75
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "First Price",
                Binding = new Binding("first_price"),
                Width = 80
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "First Δ",
                Binding = new Binding("first_delta_lakh"),
                Width = 75
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Trough",
                Binding = new Binding("trough_price"),
                Width = 80
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Corr %",
                Binding = new Binding("correction_percent"),
                Width = 70
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Second Time",
                Binding = new Binding("second_time") { StringFormat = "HH:mm" },
                Width = 80
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Second Price",
                Binding = new Binding("second_price"),
                Width = 85
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Second Δ",
                Binding = new Binding("second_delta_lakh"),
                Width = 80
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Δ Inc %",
                Binding = new Binding("delta_increase_percent"),
                Width = 75
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Time Gap",
                Binding = new Binding("time_gap_minutes"),
                Width = 75
            });

            CupPatternGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Suggestion",
                Binding = new Binding("suggestion"),
                Width = 130
            });

            // Bind data
            CupPatternGrid.ItemsSource = dataTable.DefaultView;

            // Optional: Show success message
            MessageBox.Show($"DataGrid loaded with {dataTable.Rows.Count} patterns");
        }
        // Simple Export to CSV

        //        private async void ScanDeltaSpikes_Click(object sender, RoutedEventArgs e)
        //        {




        //            if (Timecom.SelectedItem == null)
        //            {
        //                MessageBox.Show("Please select Time Interval.");
        //                return;
        //            }
        //            if (EndDatePicker.SelectedDate == null)
        //            {
        //                MessageBox.Show("Please select End Date.");
        //                return;
        //            }

        //            var selectedDate = EndDatePicker.SelectedDate.Value;
        //            var selectedTime = DateTime.ParseExact(EndTimeComboBox.SelectedItem.ToString(), "HH:mm:ss", CultureInfo.InvariantCulture);
        //            var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);

        //            var startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                                  firsttime.Hour, firsttime.Minute, firsttime.Second);

        //            var endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                                  selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

        //            int aggmin = (int)Timecom.SelectedItem;
        //            var startUtc = startIST.ToUniversalTime();
        //            var endUtc = endIST.ToUniversalTime();

        //            var con = new NpgsqlConnection(_connectionString);
        //            await con.OpenAsync();

        //            string deltaCheckQuery = @"
        //WITH ticks AS (
        //    SELECT 
        //        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        //        ts,
        //        instrument_name,
        //        price,
        //        size,
        //        CASE 
        //            WHEN price > LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
        //            ELSE 0 
        //        END AS tick_buy,
        //        CASE 
        //            WHEN price < LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
        //            ELSE 0 
        //        END AS tick_sell
        //    FROM raw_ticks
        //    WHERE ts BETWEEN @startUtc AND @endUtc
        //      AND instrument_name IN (
        //          SELECT DISTINCT instrument_name 
        //          FROM raw_ticks 
        //          WHERE ts BETWEEN @startUtc AND @endUtc 
        //          LIMIT 5
        //      )
        //),

        //bucketed AS (
        //    SELECT 
        //        instrument_name,
        //        (date_trunc('minute', ts_ist) 
        //            - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS candle_time,
        //        SUM(size) AS total_volume,
        //        SUM(tick_buy) AS total_buy_delta,
        //        SUM(tick_sell) AS total_sell_delta,
        //        (SUM(tick_buy) - SUM(tick_sell)) AS net_delta,
        //        (SUM(tick_buy) - SUM(tick_sell)) / 100000.0 AS net_delta_lakh
        //    FROM ticks
        //    GROUP BY instrument_name, 
        //            (date_trunc('minute', ts_ist) 
        //             - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin)))
        //)

        //SELECT 
        //    instrument_name,
        //    candle_time,
        //    total_volume,
        //    total_buy_delta,
        //    total_sell_delta,
        //    net_delta,
        //    net_delta_lakh,
        //    CASE 
        //        WHEN net_delta_lakh > 2 THEN 'HIGH_BUYING'
        //        WHEN net_delta_lakh < -2 THEN 'HIGH_SELLING'
        //        WHEN net_delta_lakh > 1 THEN 'MOD_BUYING'
        //        WHEN net_delta_lakh < -1 THEN 'MOD_SELLING'
        //        ELSE 'NEUTRAL'
        //    END AS delta_strength
        //FROM bucketed
        //WHERE ABS(net_delta_lakh) > 0.5  -- Filter for some delta activity
        //ORDER BY ABS(net_delta_lakh) DESC, candle_time
        //LIMIT 50;
        //";

        //            var cmd1 = new NpgsqlCommand(deltaCheckQuery, con);
        //            cmd1.Parameters.AddWithValue("startUtc", startUtc);
        //            cmd1.Parameters.AddWithValue("endUtc", endUtc);
        //            cmd1.Parameters.AddWithValue("aggmin", aggmin);

        //            var dataTable1 = new DataTable();
        //            using (var reader = await cmd1.ExecuteReaderAsync())
        //            {
        //                dataTable1.Load(reader);
        //            }

        //            if (dataTable1.Rows.Count > 0)
        //            {
        //                StockDeltaGrid.ItemsSource = dataTable1.DefaultView;
        //                MessageBox.Show($"Found {dataTable1.Rows.Count} delta entries for top 5 stocks.");

        //                // Display summary stats
        //                var maxDelta = dataTable1.AsEnumerable()
        //                    .Select(row => Convert.ToDecimal(row["net_delta_lakh"]))
        //                    .DefaultIfEmpty(0)
        //                    .Max();
        //                var minDelta = dataTable1.AsEnumerable()
        //                    .Select(row => Convert.ToDecimal(row["net_delta_lakh"]))
        //                    .DefaultIfEmpty(0)
        //                    .Min();

        //                MessageBox.Show($"Delta range: {minDelta:N2} to {maxDelta:N2} lakh");
        //            }
        //            else
        //            {
        //                MessageBox.Show("No significant delta found. Try lowering the threshold (0.5 lakh).");
        //            }

        //            con.Close();
        //            return;
        //            //AND instrument_type IN ('FUTSTK', 'FUTIDX')
        //            // Delta Spike Scanner Query
        //            string deltaSpikeQuery = @"
        //WITH stock_delta_data AS (
        //    -- Get aggregated delta data for all stocks in the selected timeframe
        //    WITH ticks AS (
        //        SELECT 
        //            ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        //            ts,
        //            instrument_name,
        //            price,
        //            size,
        //            oi,
        //            CASE 
        //                WHEN price > LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
        //                ELSE 0 
        //            END AS tick_buy,
        //            CASE 
        //                WHEN price < LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
        //                ELSE 0 
        //            END AS tick_sell
        //        FROM raw_ticks
        //        WHERE ts BETWEEN @startUtc AND @endUtc

        //    ),

        //    bucketed AS (
        //        SELECT 
        //            instrument_name,
        //            (date_trunc('minute', ts_ist) 
        //                - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))) AS candle_time,
        //            AVG(price) AS avg_price,
        //            MAX(price) AS high_price,
        //            MIN(price) AS low_price,
        //            SUM(size) AS total_volume,
        //            SUM(tick_buy) AS total_buy_delta,
        //            SUM(tick_sell) AS total_sell_delta,
        //            MAX(oi) AS last_oi,
        //            row_number() OVER (
        //                PARTITION BY instrument_name, 
        //                (date_trunc('minute', ts_ist) 
        //                 - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin)))
        //                ORDER BY ts_ist DESC
        //            ) AS rn_close
        //        FROM ticks
        //        GROUP BY instrument_name, 
        //                (date_trunc('minute', ts_ist) 
        //                 - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))),
        //                ts_ist
        //    ),

        //    delta_calcs AS (
        //        SELECT 
        //            instrument_name,
        //            candle_time,
        //            AVG(avg_price) AS avg_price,
        //            MAX(high_price) AS high_price,
        //            MIN(low_price) AS low_price,
        //            SUM(total_volume) AS total_volume,
        //            SUM(total_buy_delta) AS total_buy_delta,
        //            SUM(total_sell_delta) AS total_sell_delta,
        //            (SUM(total_buy_delta) - SUM(total_sell_delta)) AS net_delta,
        //            (SUM(total_buy_delta) - SUM(total_sell_delta)) / 100000.0 AS net_delta_lakh,
        //            MAX(CASE WHEN rn_close = 1 THEN last_oi END) AS last_oi
        //        FROM bucketed
        //        GROUP BY instrument_name, candle_time
        //    ),

        //    delta_stats AS (
        //        SELECT 
        //            *,
        //            -- Calculate price zones (rounded to nearest 0.5% for grouping)
        //            ROUND(avg_price / (avg_price * 0.005)) * (avg_price * 0.005) AS price_zone,
        //            -- Calculate rolling statistics for z-score
        //            AVG(net_delta_lakh) OVER (
        //                PARTITION BY instrument_name 
        //                ORDER BY candle_time 
        //                ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        //            ) AS avg_delta_20period,
        //            STDDEV(net_delta_lakh) OVER (
        //                PARTITION BY instrument_name 
        //                ORDER BY candle_time 
        //                ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        //            ) AS stddev_delta_20period
        //        FROM delta_calcs
        //    )

        //    SELECT 
        //        *,
        //        -- Calculate delta z-score
        //        (net_delta_lakh - avg_delta_20period) / NULLIF(stddev_delta_20period, 0) AS delta_zscore
        //    FROM delta_stats
        //),

        //-- Find delta spikes
        //delta_spikes AS (
        //    SELECT 
        //        *,
        //        LAG(price_zone) OVER (PARTITION BY instrument_name ORDER BY candle_time) AS prev_price_zone,
        //        LAG(net_delta_lakh) OVER (PARTITION BY instrument_name ORDER BY candle_time) AS prev_delta,
        //        LAG(avg_price) OVER (PARTITION BY instrument_name ORDER BY candle_time) AS prev_avg_price,
        //        LAG(candle_time) OVER (PARTITION BY instrument_name ORDER BY candle_time) AS prev_candle_time
        //    FROM stock_delta_data
        //    WHERE ABS(net_delta_lakh) >= 2.0  -- Minimum 2 lakh delta
        //),

        //-- Group stocks by price zone and time
        //price_zone_groups AS (
        //    SELECT 
        //        price_zone,
        //        candle_time,
        //        COUNT(DISTINCT instrument_name) AS stock_count,
        //        STRING_AGG(instrument_name, ', ') AS stocks,
        //        AVG(net_delta_lakh) AS avg_delta,
        //        MAX(net_delta_lakh) AS max_delta,
        //        MIN(net_delta_lakh) AS min_delta
        //    FROM delta_spikes
        //    WHERE ABS(delta_zscore) >= 1.5 OR ABS(net_delta_lakh) >= 3.0
        //    GROUP BY price_zone, candle_time
        //    HAVING COUNT(DISTINCT instrument_name) >= 2  -- At least 2 stocks in same zone
        //),

        //-- Find consecutive spikes in same price zone
        //consecutive_spikes AS (
        //    SELECT 
        //        pzg1.price_zone,
        //        pzg1.candle_time AS first_spike_time,
        //        pzg2.candle_time AS second_spike_time,
        //        pzg1.stocks AS first_spike_stocks,
        //        pzg2.stocks AS second_spike_stocks,
        //        pzg1.avg_delta AS first_avg_delta,
        //        pzg2.avg_delta AS second_avg_delta,
        //        pzg1.max_delta AS first_max_delta,
        //        pzg2.max_delta AS second_max_delta,
        //        EXTRACT(EPOCH FROM (pzg2.candle_time - pzg1.candle_time)) / 60 AS minutes_between,
        //        -- Check if same stocks are involved
        //        (SELECT COUNT(*) FROM unnest(string_to_array(pzg1.stocks, ', ')) 
        //         INTERSECT 
        //         SELECT COUNT(*) FROM unnest(string_to_array(pzg2.stocks, ', '))) AS common_stocks_count
        //    FROM price_zone_groups pzg1
        //    JOIN price_zone_groups pzg2 
        //        ON pzg1.price_zone = pzg2.price_zone
        //        AND pzg2.candle_time > pzg1.candle_time
        //        AND EXTRACT(EPOCH FROM (pzg2.candle_time - pzg1.candle_time)) BETWEEN 5 * 60 AND 60 * 60  -- 5 min to 1 hour
        //    WHERE pzg1.candle_time >= (SELECT MIN(candle_time) FROM delta_spikes) + INTERVAL '20 minutes'
        //)

        //-- Final results
        //SELECT 
        //    cs.price_zone,
        //    cs.first_spike_time,
        //    cs.second_spike_time,
        //    cs.minutes_between,
        //    cs.first_spike_stocks,
        //    cs.second_spike_stocks,
        //    ROUND(cs.first_avg_delta, 2) AS first_avg_delta_lakh,
        //    ROUND(cs.second_avg_delta, 2) AS second_avg_delta_lakh,
        //    ROUND(cs.first_max_delta, 2) AS first_max_delta_lakh,
        //    ROUND(cs.second_max_delta, 2) AS second_max_delta_lakh,
        //    cs.common_stocks_count,
        //    -- Get sample stock details for display
        //    (SELECT instrument_name FROM delta_spikes ds 
        //     WHERE ds.price_zone = cs.price_zone 
        //       AND ds.candle_time = cs.first_spike_time 
        //     LIMIT 1) AS sample_stock,
        //    (SELECT avg_price FROM delta_spikes ds 
        //     WHERE ds.price_zone = cs.price_zone 
        //       AND ds.candle_time = cs.first_spike_time 
        //     LIMIT 1) AS sample_price,
        //    -- Strength indicator
        //    CASE 
        //        WHEN cs.common_stocks_count >= 2 AND cs.second_avg_delta > cs.first_avg_delta THEN 'STRONG_CONFIRMATION'
        //        WHEN cs.common_stocks_count >= 1 THEN 'MODERATE_CONFIRMATION'
        //        ELSE 'WEAK_CONFIRMATION'
        //    END AS confirmation_strength,
        //    -- Direction
        //    CASE 
        //        WHEN cs.first_avg_delta > 0 AND cs.second_avg_delta > 0 THEN 'BULLISH'
        //        WHEN cs.first_avg_delta < 0 AND cs.second_avg_delta < 0 THEN 'BEARISH'
        //        ELSE 'MIXED'
        //    END AS direction
        //FROM consecutive_spikes cs
        //WHERE cs.minutes_between BETWEEN 5 AND 30  -- Focus on 5-30 minute intervals
        //ORDER BY cs.first_spike_time DESC, ABS(cs.first_avg_delta + cs.second_avg_delta) DESC
        //LIMIT 100;
        //";

        //            try
        //            {
        //                var cmd = new NpgsqlCommand(deltaSpikeQuery, con);
        //                cmd.Parameters.AddWithValue("startUtc", startUtc);
        //                cmd.Parameters.AddWithValue("endUtc", endUtc);
        //                cmd.Parameters.AddWithValue("aggmin", aggmin);

        //                cmd.CommandTimeout = 180; // 3 minutes timeout for scanning all stocks

        //                // Create DataTable to store results
        //                var dataTable = new DataTable();
        //                using (var reader = await cmd.ExecuteReaderAsync())
        //                {
        //                    dataTable.Load(reader);
        //                }

        //                // Display results in DataGrid
        //                if (dataTable.Rows.Count > 0)
        //                {
        //                    DeltaSpikeGrid.ItemsSource = dataTable.DefaultView;

        //                    // Show summary
        //                    MessageBox.Show($"Found {dataTable.Rows.Count} delta spike patterns in selected timeframe.");

        //                    // Export to CSV for backtesting analysis
        //                    //ExportToCSV(dataTable, $"DeltaSpikes_{selectedDate:yyyyMMdd}_{aggmin}min.csv");
        //                }
        //                else
        //                {
        //                    MessageBox.Show("No delta spike patterns found in the selected timeframe.");
        //                    DeltaSpikeGrid.ItemsSource = null;
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                MessageBox.Show($"Error scanning delta spikes: {ex.Message}");
        //            }
        //            finally
        //            {
        //                con.Close();
        //            }
        //        }

        // Helper method to export results to CSV
        //private void ExportToCSV(DataTable dataTable, string fileName)
        //{
        //    try
        //    {
        //        var sb = new StringBuilder();

        //        // Add headers
        //        var columnNames = dataTable.Columns.Cast<DataColumn>()
        //                                .Select(column => column.ColumnName);
        //        sb.AppendLine(string.Join(",", columnNames));

        //        // Add rows
        //        foreach (DataRow row in dataTable.Rows)
        //        {
        //            var fields = row.ItemArray.Select(field =>
        //                field?.ToString().Contains(",") == true ?
        //                $"\"{field}\"" :
        //                field?.ToString() ?? "");
        //            sb.AppendLine(string.Join(",", fields));
        //        }

        //        var filePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), fileName);
        //        File.WriteAllText(filePath, sb.ToString());

        //        // Optional: Show export confirmation
        //        // MessageBox.Show($"Results exported to: {filePath}");
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"Error exporting to CSV: {ex.Message}");
        //    }
        //}

        // Class for delta spike results (optional, for type-safe handling)
        public class DeltaSpikeResult
        {
            public decimal PriceZone { get; set; }
            public DateTime FirstSpikeTime { get; set; }
            public DateTime SecondSpikeTime { get; set; }
            public decimal MinutesBetween { get; set; }
            public string FirstSpikeStocks { get; set; }
            public string SecondSpikeStocks { get; set; }
            public decimal FirstAvgDelta { get; set; }
            public decimal SecondAvgDelta { get; set; }
            public decimal FirstMaxDelta { get; set; }
            public decimal SecondMaxDelta { get; set; }
            public int CommonStocksCount { get; set; }
            public string SampleStock { get; set; }
            public decimal SamplePrice { get; set; }
            public string ConfirmationStrength { get; set; }
            public string Direction { get; set; }
        }

        // Alternative method to get typed results
        private async Task<List<DeltaSpikeResult>> GetDeltaSpikeResults(DateTime startUtc, DateTime endUtc, int aggmin)
        {
            var results = new List<DeltaSpikeResult>();

            using (var con = new NpgsqlConnection(_connectionString))
            {
                await con.OpenAsync();

                var cmd = new NpgsqlCommand(_connectionString, con);
                cmd.Parameters.AddWithValue("startUtc", startUtc);
                cmd.Parameters.AddWithValue("endUtc", endUtc);
                cmd.Parameters.AddWithValue("aggmin", aggmin);

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var result = new DeltaSpikeResult
                        {
                            PriceZone = reader.GetDecimal(0),
                            FirstSpikeTime = reader.GetDateTime(1),
                            SecondSpikeTime = reader.GetDateTime(2),
                            MinutesBetween = reader.GetDecimal(3),
                            FirstSpikeStocks = reader.GetString(4),
                            SecondSpikeStocks = reader.GetString(5),
                            FirstAvgDelta = reader.GetDecimal(6),
                            SecondAvgDelta = reader.GetDecimal(7),
                            FirstMaxDelta = reader.GetDecimal(8),
                            SecondMaxDelta = reader.GetDecimal(9),
                            CommonStocksCount = reader.GetInt32(10),
                            SampleStock = reader.IsDBNull(11) ? "" : reader.GetString(11),
                            SamplePrice = reader.IsDBNull(12) ? 0 : reader.GetDecimal(12),
                            ConfirmationStrength = reader.GetString(13),
                            Direction = reader.GetString(14)
                        };

                        results.Add(result);
                    }
                }
            }

            return results;
        }

        //        private async void ScanCvdSpikesButton_Click(object sender, RoutedEventArgs e)
        //        {
        //            try
        //            {
        //                double spikeThreshold = 10.0;
        //                double maxPriceMovePercent = 0.3;

        //                var selectedDate = DateTime.Today;
        //                if (EndDatePicker.SelectedDate != null)
        //                {
        //                    selectedDate = EndDatePicker.SelectedDate.Value;
        //                }

        //                var startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day, 9, 15, 0);
        //                var endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day, 15, 30, 0);

        //                var startUtc = startIST.ToUniversalTime();
        //                var endUtc = endIST.ToUniversalTime();

        //                using (var con = new NpgsqlConnection(_connectionString))
        //                {
        //                    await con.OpenAsync();

        //                    // SIMPLIFIED QUERY - Focus on core CVD spike detection
        //                    string sql = @"
        //WITH tick_data AS (
        //    SELECT 
        //        instrument_name,
        //        ts AT TIME ZONE 'Asia/Kolkata' as ts_ist,
        //        price,
        //        size,
        //        oi,
        //        -- Tick direction
        //        CASE 
        //            WHEN price > LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN 'BUY'
        //            WHEN price < LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN 'SELL'
        //            ELSE 'UNCHANGED'
        //        END as tick_dir
        //    FROM raw_ticks
        //    WHERE ts >= @startUtc AND ts <= @endUtc
        //),
        //minute_data AS (
        //    SELECT 
        //        instrument_name,
        //        DATE_TRUNC('minute', ts_ist) as candle_time,
        //        -- Get first price as open
        //        FIRST_VALUE(price) OVER w as open_price,
        //        -- Get last price as close
        //        LAST_VALUE(price) OVER w as close_price,
        //        -- Get high and low
        //        MAX(price) OVER w as high_price,
        //        MIN(price) OVER w as low_price,
        //        -- Calculate volume and delta
        //        SUM(size) OVER w as volume,
        //        SUM(CASE WHEN tick_dir = 'BUY' THEN size ELSE 0 END) OVER w as buy_volume,
        //        SUM(CASE WHEN tick_dir = 'SELL' THEN size ELSE 0 END) OVER w as sell_volume,
        //        -- Get last OI
        //        LAST_VALUE(oi) OVER w as last_oi
        //    FROM tick_data
        //    WINDOW w AS (PARTITION BY instrument_name, DATE_TRUNC('minute', ts_ist) 
        //                 ORDER BY ts_ist 
        //                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        //),
        //unique_minutes AS (
        //    SELECT DISTINCT
        //        instrument_name,
        //        candle_time,
        //        open_price,
        //        close_price,
        //        high_price,
        //        low_price,
        //        volume,
        //        buy_volume,
        //        sell_volume,
        //        (buy_volume - sell_volume) as minute_delta,
        //        last_oi
        //    FROM minute_data
        //),
        //cvd_calc AS (
        //    SELECT 
        //        *,
        //        SUM(minute_delta) OVER (PARTITION BY instrument_name ORDER BY candle_time) as cumulative_cvd,
        //        AVG(ABS(minute_delta)) OVER (
        //            PARTITION BY instrument_name 
        //            ORDER BY candle_time 
        //            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        //        ) as avg_cvd_20min,
        //        ABS((close_price - open_price) * 100.0 / NULLIF(open_price, 0)) as price_move_pct,
        //        last_oi - LAG(last_oi) OVER (PARTITION BY instrument_name ORDER BY candle_time) as oi_change
        //    FROM unique_minutes
        //),
        //spike_check AS (
        //    SELECT 
        //        *,
        //        CASE 
        //            WHEN avg_cvd_20min > 0 
        //            THEN ABS(minute_delta) / avg_cvd_20min 
        //            ELSE 0 
        //        END as cvd_spike_ratio
        //    FROM cvd_calc
        //    WHERE avg_cvd_20min IS NOT NULL
        //)
        //SELECT 
        //    instrument_name,
        //    candle_time,
        //    open_price,
        //    close_price,
        //    high_price,
        //    low_price,
        //    volume,
        //    minute_delta,
        //    cumulative_cvd,
        //    avg_cvd_20min,
        //    cvd_spike_ratio,
        //    price_move_pct,
        //    oi_change,
        //    CASE 
        //        WHEN minute_delta > 0 THEN 'BUY_SPIKE'
        //        ELSE 'SELL_SPIKE'
        //    END as spike_direction,
        //    CASE 
        //        WHEN minute_delta > 0 AND ABS(minute_delta) > 50000 THEN 'Strong Buyers'
        //        WHEN minute_delta > 0 AND ABS(minute_delta) > 10000 THEN 'Buyers Aggressive'
        //        WHEN minute_delta < 0 AND ABS(minute_delta) > 50000 THEN 'Strong Sellers'
        //        WHEN minute_delta < 0 AND ABS(minute_delta) > 10000 THEN 'Sellers Aggressive'
        //        ELSE 'Neutral'
        //    END as delta_sentiment,
        //    -- SIMPLE TRADING SIGNAL
        //    CASE 
        //        WHEN cvd_spike_ratio >= @spikeThreshold 
        //             AND price_move_pct <= @maxPriceMovePct
        //             AND minute_delta > 0
        //             AND oi_change > 0
        //            THEN 'LONG BREAKOUT SETUP'
        //        WHEN cvd_spike_ratio >= @spikeThreshold 
        //             AND price_move_pct <= @maxPriceMovePct
        //             AND minute_delta < 0
        //             AND oi_change > 0
        //            THEN 'SHORT BREAKOUT SETUP'
        //        ELSE 'NO_SIGNAL'
        //    END as trading_signal
        //FROM spike_check
        //WHERE cvd_spike_ratio >= @spikeThreshold 
        //  AND price_move_pct <= @maxPriceMovePct
        //  AND ABS(minute_delta) > 10000
        //ORDER BY cvd_spike_ratio DESC, candle_time DESC
        //LIMIT 100;";

        //                    using (var cmd = new NpgsqlCommand(sql, con))
        //                    {
        //                        cmd.Parameters.AddWithValue("@startUtc", startUtc);
        //                        cmd.Parameters.AddWithValue("@endUtc", endUtc);
        //                        cmd.Parameters.AddWithValue("@spikeThreshold", spikeThreshold);
        //                        cmd.Parameters.AddWithValue("@maxPriceMovePct", maxPriceMovePercent);

        //                        var dt = new DataTable();
        //                        using (var reader = await cmd.ExecuteReaderAsync())
        //                        {
        //                            dt.Load(reader);
        //                        }

        //                        DataGridCVDSpikes.ItemsSource = dt.DefaultView;
        //                        MessageBox.Show($"Found {dt.Rows.Count} CVD spike signals");
        //                    }
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                MessageBox.Show($"Error: {ex.Message}");
        //            }
        //        }

        private async void ScanCvdSpikesButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                double spikeThreshold = 10.0;
                double maxPriceMovePercent = 0.3;

                var selectedDate = DateTime.Today;
                if (EndDatePicker.SelectedDate != null)
                {
                    selectedDate = EndDatePicker.SelectedDate.Value;
                }

                var startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day, 9, 15, 0);
                var endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day, 15, 30, 0);

                var startUtc = startIST.ToUniversalTime();
                var endUtc = endIST.ToUniversalTime();

                using (var con = new NpgsqlConnection(_connectionString))
                {
                    await con.OpenAsync();

                    // CLEAN, WORKING VERSION - No ROUND() function errors
                    string sql = @"
-- Step 1: Basic CVD calculation
WITH tick_cvd AS (
    SELECT 
        instrument_name,
        ts,
        price,
        size,
        CASE 
            WHEN price > LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN size
            WHEN price < LAG(price) OVER (PARTITION BY instrument_name ORDER BY ts) THEN -size
            ELSE 0 
        END as tick_cvd
    FROM raw_ticks
    WHERE ts >= @startUtc AND ts <= @endUtc
),
-- Step 2: Create 15-minute candles
fifteen_min_buckets AS (
    SELECT 
        instrument_name,
        -- Create 15-minute buckets
        DATE_TRUNC('hour', ts) + 
            FLOOR(EXTRACT(MINUTE FROM ts) / 15) * INTERVAL '15 minutes' as bucket_time,
        price,
        tick_cvd,
        size,
        ROW_NUMBER() OVER (PARTITION BY instrument_name, 
                          DATE_TRUNC('hour', ts) + 
                          FLOOR(EXTRACT(MINUTE FROM ts) / 15) * INTERVAL '15 minutes' 
                          ORDER BY ts) as rn_asc,
        ROW_NUMBER() OVER (PARTITION BY instrument_name, 
                          DATE_TRUNC('hour', ts) + 
                          FLOOR(EXTRACT(MINUTE FROM ts) / 15) * INTERVAL '15 minutes' 
                          ORDER BY ts DESC) as rn_desc
    FROM tick_cvd
),
-- Step 3: Aggregate to get candle data
candle_data AS (
    SELECT 
        instrument_name,
        bucket_time,
        MAX(CASE WHEN rn_asc = 1 THEN price END) as open_price,
        MAX(CASE WHEN rn_desc = 1 THEN price END) as close_price,
        MAX(price) as high_price,
        MIN(price) as low_price,
        SUM(tick_cvd) as fifteen_min_cvd,
        SUM(size) as volume
    FROM fifteen_min_buckets
    GROUP BY instrument_name, bucket_time
),
-- Step 4: Calculate rolling average
with_stats AS (
    SELECT 
        *,
        AVG(ABS(fifteen_min_cvd)) OVER (
            PARTITION BY instrument_name 
            ORDER BY bucket_time 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as avg_cvd_20
    FROM candle_data
)
-- Step 5: Find spikes
SELECT 
    instrument_name,
    TO_CHAR(bucket_time AT TIME ZONE 'Asia/Kolkata', 'HH24:MI') as candle_time,
    open_price,
    close_price,
    high_price,
    low_price,
    fifteen_min_cvd as delta,
    volume,
    avg_cvd_20,
    -- Calculate spike ratio without ROUND()
    CASE 
        WHEN avg_cvd_20 > 0 
        THEN ABS(fifteen_min_cvd) / avg_cvd_20 
        ELSE 0 
    END as spike_ratio,
    -- Calculate price movement percentage
    CASE 
        WHEN open_price > 0 
        THEN ABS((close_price - open_price) * 100.0 / open_price)
        ELSE 0 
    END as price_move_pct,
    -- Simple signal
    CASE 
        WHEN avg_cvd_20 > 0 
             AND ABS(fifteen_min_cvd) / avg_cvd_20 >= @spikeThreshold 
             AND ABS((close_price - open_price) * 100.0 / open_price) <= @maxPriceMovePct
             AND fifteen_min_cvd > 0
            THEN 'LONG BREAKOUT'
        WHEN avg_cvd_20 > 0 
             AND ABS(fifteen_min_cvd) / avg_cvd_20 >= @spikeThreshold 
             AND ABS((close_price - open_price) * 100.0 / open_price) <= @maxPriceMovePct
             AND fifteen_min_cvd < 0
            THEN 'SHORT BREAKOUT'
        ELSE 'NO SIGNAL'
    END as signal
FROM with_stats
WHERE avg_cvd_20 IS NOT NULL
  AND avg_cvd_20 > 0
  AND fifteen_min_cvd != 0
  AND bucket_time >= @startUtc
  AND bucket_time <= @endUtc
ORDER BY 
    CASE 
        WHEN avg_cvd_20 > 0 THEN ABS(fifteen_min_cvd) / avg_cvd_20 
        ELSE 0 
    END DESC,
    bucket_time DESC
LIMIT 100;";

                    using (var cmd = new NpgsqlCommand(sql, con))
                    {
                        cmd.Parameters.AddWithValue("@startUtc", startUtc);
                        cmd.Parameters.AddWithValue("@endUtc", endUtc);
                        cmd.Parameters.AddWithValue("@spikeThreshold", spikeThreshold);
                        cmd.Parameters.AddWithValue("@maxPriceMovePct", maxPriceMovePercent);

                        // Load DataTable
                        var dt = new DataTable();
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            dt.Load(reader);
                        }

                        // Ensure writable columns (prevents ReadOnlyException)
                        dt.Columns["spike_ratio"].ReadOnly = false;
                        dt.Columns["price_move_pct"].ReadOnly = false;

                        // Create delta_lakhs column if missing
                        if (!dt.Columns.Contains("delta_lakhs"))
                        {
                            dt.Columns.Add("delta_lakhs", typeof(double));
                        }

                        // Format the data
                        foreach (DataRow row in dt.Rows)
                        {
                            // spike_ratio → 1 decimal
                            if (row["spike_ratio"] != DBNull.Value)
                            {
                                double spikeRatio = Convert.ToDouble(row["spike_ratio"]);
                                row["spike_ratio"] = Math.Round(spikeRatio, 1);
                            }

                            // price_move_pct → 3 decimals
                            if (row["price_move_pct"] != DBNull.Value)
                            {
                                double priceMove = Convert.ToDouble(row["price_move_pct"]);
                                row["price_move_pct"] = Math.Round(priceMove, 3);
                            }

                            // delta_lakhs → delta / 1,00,000 (2 decimals)
                            if (row["delta"] != DBNull.Value)
                            {
                                double delta = Convert.ToDouble(row["delta"]);
                                row["delta_lakhs"] = Math.Round(delta / 100000.0, 2);
                            }
                        }


                        DataGridCVDSpikes.ItemsSource = dt.DefaultView;

                        if (dt.Rows.Count > 0)
                        {
                            int totalSignals = dt.Rows.Count;
                            int longSignals = dt.AsEnumerable()
                                .Count(row => row["signal"].ToString() == "LONG BREAKOUT");
                            int shortSignals = dt.AsEnumerable()
                                .Count(row => row["signal"].ToString() == "SHORT BREAKOUT");

                            MessageBox.Show($"15-Minute CVD Spike Scanner\n\n" +
                                          $"Total Signals: {totalSignals}\n" +
                                          $"Long Breakouts: {longSignals}\n" +
                                          $"Short Breakouts: {shortSignals}");
                        }
                        else
                        {
                            MessageBox.Show("No CVD spikes found. Try:\n" +
                                          "1. Reduce spike threshold to 5\n" +
                                          "2. Increase price move to 0.5%\n" +
                                          "3. Check if market data exists");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error: {ex.Message}");
            }
        }
        private void EndDatePicker_SelectedDateChanged(object sender, SelectionChangedEventArgs e)
        {

        }

        private async void cvd_click1(object sender, RoutedEventArgs e)
        {
            if (InstrumentCombo.SelectedItem == null)
            {
                MessageBox.Show("Please select an instrument.");
                return;
            }
            if (Timecom.SelectedItem == null)
            {
                MessageBox.Show("Please select Time Interval.");
                return;
            }

            try
            {
                var selectedDate = EndDatePicker.SelectedDate.Value;

                var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);
                var selectedTime = DateTime.ParseExact("15:30:00", "HH:mm:ss", CultureInfo.InvariantCulture);

                startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                      firsttime.Hour, firsttime.Minute, firsttime.Second);

                endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
                                      selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

                string symbol = InstrumentCombo.SelectedItem.ToString();
                int aggmin = (int)Timecom.SelectedItem; // keep as you had it
                var startUtc = startIST.ToUniversalTime();
                var endUtc = endIST.ToUniversalTime();

                using (var con = new NpgsqlConnection(_connectionString))
                {
                    await con.OpenAsync();

                    string sql = @"
WITH ticks AS (
    SELECT
        ts,
        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        price,
        size,
        bid_price,
        ask_price,
        bid_qty,
        ask_qty,
        oi,
        order_imbalance,
        cvd AS true_cvd_tick,     -- you already have true CVD per tick

        /* --------------------------
           PRICE-BASED CVD
        --------------------------- */
        CASE 
            WHEN price > LAG(price) OVER (ORDER BY ts) THEN size
            ELSE 0
        END AS price_buy,

        CASE 
            WHEN price < LAG(price) OVER (ORDER BY ts) THEN size
            ELSE 0
        END AS price_sell,


        /* --------------------------
           TRUE AGGRESSOR CVD
        --------------------------- */
        CASE 
            WHEN price = ask_price THEN size    -- aggressive buyer
            ELSE 0
        END AS true_buy,

        CASE 
            WHEN price = bid_price THEN size    -- aggressive seller
            ELSE 0
        END AS true_sell,


        /* --------------------------
           ABSORPTION DETECTION
           If large bid/ask qty gets eaten by small price move
        --------------------------- */
        CASE 
            WHEN price = ask_price AND ask_qty > size * 3 THEN 'Ask Absorption'
            WHEN price = bid_price AND bid_qty > size * 3 THEN 'Bid Absorption'
            ELSE 'None'
        END AS absorption_type,


        /* --------------------------
           SPOOFING PROBABILITY
           If qty suddenly appears & disappears without trades
        --------------------------- */
        CASE
            WHEN (ask_qty + bid_qty) > 0 AND size = 0
                 AND (ask_qty > LAG(ask_qty) OVER (ORDER BY ts) * 2
                      OR bid_qty > LAG(bid_qty) OVER (ORDER BY ts) * 2)
            THEN 'Possible Spoof'
            ELSE 'Normal'
        END AS spoof_flag

    FROM raw_ticks
    WHERE instrument_name = @instrumentName
      AND ts BETWEEN @startUtc AND @endUtc
)";

                    using (var cmd = new NpgsqlCommand(sql, con))
                    {
                        cmd.Parameters.AddWithValue("instrumentName", symbol);
                        cmd.Parameters.AddWithValue("startUtc", startUtc);
                        cmd.Parameters.AddWithValue("endUtc", endUtc);
                        cmd.Parameters.AddWithValue("aggmin", aggmin);

                        var dt = new DataTable();
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            dt.Load(reader);
                        }

                        DataGridCVD.ItemsSource = dt.DefaultView;
                        MessageBox.Show("Done");
                    }
                }
            }
            catch (Exception ex)
            {
                //MessageBox.Show($"Error:"  ex.Message};
            }
        }
        private async void cvd_click(object sender, RoutedEventArgs e)
        {
            if (InstrumentCombo.SelectedItem == null)
            {
                MessageBox.Show("Please select an instrument.");
                return;
            }
            if (Timecom.SelectedItem == null)
            {
                MessageBox.Show("Please select Time Interval.");
                return;
            }

            try
            {
                var selectedDate = EndDatePicker.SelectedDate.Value.Date;

                var startIST = DateTime.Parse($"{selectedDate:yyyy-MM-dd} 09:15:00");
                var endIST = DateTime.Parse($"{selectedDate:yyyy-MM-dd} 15:30:00");

                var startUtc = startIST.ToUniversalTime();
                var endUtc = endIST.ToUniversalTime();

                string symbol = InstrumentCombo.SelectedItem.ToString();
                int aggmin = Convert.ToInt32(Timecom.SelectedItem);

                using (var con = new NpgsqlConnection(_connectionString))
                {
                    await con.OpenAsync();

                    string sql = @"
WITH
-- 1) Tick-level classification
ticks AS (
    SELECT
        ts,
        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        instrument_key,
        instrument_name,
        price,
        size,
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        oi,
        cvd AS true_cvd_tick,

        CASE WHEN price > LAG(price) OVER (ORDER BY ts) THEN size ELSE 0 END AS price_buy,
        CASE WHEN price < LAG(price) OVER (ORDER BY ts) THEN size ELSE 0 END AS price_sell,

        CASE WHEN price = ask_price THEN size ELSE 0 END AS true_buy,
        CASE WHEN price = bid_price THEN size ELSE 0 END AS true_sell,

        CASE WHEN price > (bid_price + ask_price)/2 THEN size ELSE 0 END AS hybrid_buy,
        CASE WHEN price < (bid_price + ask_price)/2 THEN size ELSE 0 END AS hybrid_sell
    FROM raw_ticks
    WHERE instrument_name = @instrumentName
      AND ts BETWEEN @startUtc AND @endUtc
),

-- 2) Add candle_time
bucketed AS (
    SELECT
        *,
        (
            date_trunc('minute', ts_ist)
            - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin)
        ) AS candle_time
    FROM ticks
),

-- 3) Row numbers for OHLC
ordered_ticks AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY candle_time ORDER BY ts_ist ASC)  AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY candle_time ORDER BY ts_ist DESC) AS rn_desc
    FROM bucketed
),

-- 4) Candle aggregation
candles AS (
    SELECT
        candle_time,

        MAX(price) FILTER (WHERE rn_asc = 1) AS open_price,
        MAX(price) AS high_price,
        MIN(price) AS low_price,
        MAX(price) FILTER (WHERE rn_desc = 1) AS close_price,

        SUM(size) AS total_volume,
        MAX(oi) AS last_oi,

        SUM(price_buy)  AS price_buy_delta,
        SUM(price_sell) AS price_sell_delta,

        SUM(true_buy)   AS true_buy_delta,
        SUM(true_sell)  AS true_sell_delta,

        SUM(hybrid_buy) AS hybrid_buy_delta,
        SUM(hybrid_sell) AS hybrid_sell_delta,

        MAX(true_cvd_tick) AS true_cvd_point
    FROM ordered_ticks
    GROUP BY candle_time
    ORDER BY candle_time
),

-- 5) Add cumulative CVDs
cvd_running AS (
    SELECT
        *,
        SUM(price_buy_delta - price_sell_delta)
            OVER (ORDER BY candle_time) AS cum_price_delta,

        SUM(true_buy_delta - true_sell_delta)
            OVER (ORDER BY candle_time) AS cum_true_delta,

        SUM(hybrid_buy_delta - hybrid_sell_delta)
            OVER (ORDER BY candle_time) AS cum_hybrid_delta
    FROM candles
),

-- 6) Final CVD + divergence
final_cvd AS (
    SELECT
        candle_time,
        open_price, high_price, low_price, close_price,
        total_volume, last_oi,

        (price_buy_delta - price_sell_delta) AS price_delta,
        (true_buy_delta - true_sell_delta) AS true_delta,
        (hybrid_buy_delta - hybrid_sell_delta) AS hybrid_delta,

        price_buy_delta, price_sell_delta,
        true_buy_delta, true_sell_delta,
        hybrid_buy_delta, hybrid_sell_delta,

        cum_price_delta,
        cum_true_delta,
        cum_hybrid_delta,

        close_price - LAG(close_price) OVER (ORDER BY candle_time) AS price_change,
        last_oi - LAG(last_oi) OVER (ORDER BY candle_time) AS oi_change,

        LAG(close_price)        OVER (ORDER BY candle_time) AS prev_close,
        LAG(cum_true_delta)     OVER (ORDER BY candle_time) AS prev_cum_true_delta
    FROM cvd_running
)


-- RESULT 2: Candles
SELECT
    candle_time,

    ROUND(((price_buy_delta - price_sell_delta) / 100000.0)::numeric, 2) AS Price_CVD,
    ROUND(((true_buy_delta - true_sell_delta) / 100000.0)::numeric, 2) AS True_CVD,
    ROUND(((hybrid_buy_delta - hybrid_sell_delta) / 100000.0)::numeric, 2) AS Hybrid_CVD,
    ROUND((total_volume / 100000.0)::numeric, 2) AS total_volume,
    ROUND((last_oi / 100000.0)::numeric, 2) AS last_oi,
    ROUND((true_cvd_point / 100000.0)::numeric, 2) AS true_cvd_point

FROM candles
ORDER BY candle_time;
";

                    //-- RESULT 1: Ticks
                    //SELECT *
                    //FROM ticks
                    //ORDER BY ts_ist;

                    //SELECT
                    //    candle_time,
                    //    price_delta, cum_price_delta,
                    //    true_delta, cum_true_delta,
                    //    hybrid_delta, cum_hybrid_delta,
                    //    price_change, oi_change,

                    //    CASE
                    //        WHEN prev_close IS NOT NULL
                    //             AND close_price > prev_close
                    //             AND cum_true_delta < prev_cum_true_delta
                    //             THEN 'Bearish Divergence'
                    //        WHEN prev_close IS NOT NULL
                    //             AND close_price < prev_close
                    //             AND cum_true_delta > prev_cum_true_delta
                    //             THEN 'Bullish Divergence'
                    //        ELSE 'Neutral'
                    //    END AS cvd_divergence
                    //FROM final_cvd
                    //ORDER BY candle_time;

                    using (var cmd = new NpgsqlCommand(sql, con))
                    {
                        cmd.Parameters.AddWithValue("instrumentName", symbol);
                        cmd.Parameters.AddWithValue("startUtc", startUtc);
                        cmd.Parameters.AddWithValue("endUtc", endUtc);
                        cmd.Parameters.AddWithValue("aggmin", aggmin);

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            // RESULT 1: ticks
                            var ticksDT = new DataTable();
                            ticksDT.Load(reader);
                            DataGridCVDSpikes.ItemsSource = ticksDT.DefaultView;

                            //await reader.NextResultAsync();

                            //// RESULT 2: candles
                            //var candlesDT = new DataTable();
                            //candlesDT.Load(reader);
                            ////CandleGrid.ItemsSource = candlesDT.DefaultView;

                            //await reader.NextResultAsync();

                            //// RESULT 3: CVD + divergence
                            //var cvdDT = new DataTable();
                            //cvdDT.Load(reader);
                            //CVDGrid.ItemsSource = cvdDT.DefaultView;
                        }
                    }
                }

                MessageBox.Show("Orderflow CVD loaded.");
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error: {ex.Message}");
            }
        }



        //        private async void cvd_click(object sender, RoutedEventArgs e)
        //        {
        //            if (InstrumentCombo.SelectedItem == null)
        //            {
        //                MessageBox.Show("Please select an instrument.");
        //                return;
        //            }
        //            if (Timecom.SelectedItem == null)
        //            {
        //                MessageBox.Show("Please select Time Interval.");
        //                return;
        //            }

        //            try
        //            {
        //                var selectedDate = EndDatePicker.SelectedDate.Value;

        //                var firsttime = DateTime.ParseExact("09:15:00", "HH:mm:ss", CultureInfo.InvariantCulture);
        //                var selectedTime = DateTime.ParseExact("15:30:00", "HH:mm:ss", CultureInfo.InvariantCulture);

        //                startIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                                      firsttime.Hour, firsttime.Minute, firsttime.Second);

        //                endIST = new DateTime(selectedDate.Year, selectedDate.Month, selectedDate.Day,
        //                                      selectedTime.Hour, selectedTime.Minute, selectedTime.Second);

        //                string symbol = InstrumentCombo.SelectedItem.ToString();
        //                int aggmin = (int)Timecom.SelectedItem;
        //                var startUtc = startIST.ToUniversalTime();
        //                var endUtc = endIST.ToUniversalTime();

        //                using (var con = new NpgsqlConnection(_connectionString))
        //                {
        //                    await con.OpenAsync();

        //                    string sql = @"
        //-- Multi-result SQL: ticks, candles, final, absorption rows, divergence rows
        //WITH ticks AS (
        //    SELECT
        //        ts,
        //        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        //        instrument_key,
        //        instrument_name,
        //        price,
        //        size,
        //        bid_price,
        //        bid_qty,
        //        ask_price,
        //        ask_qty,
        //        oi,
        //        order_imbalance,
        //        cvd AS true_cvd_tick,     -- your True CVD column per tick

        //        /* PRICE-BASED CVD */
        //        CASE 
        //            WHEN price > LAG(price) OVER (ORDER BY ts) THEN size
        //            ELSE 0
        //        END AS price_buy,

        //        CASE 
        //            WHEN price < LAG(price) OVER (ORDER BY ts) THEN size
        //            ELSE 0
        //        END AS price_sell,

        //        /* TRUE AGGRESSOR CVD (bid/ask) */
        //        CASE 
        //            WHEN price = ask_price THEN size
        //            ELSE 0
        //        END AS true_buy,

        //        CASE 
        //            WHEN price = bid_price THEN size
        //            ELSE 0
        //        END AS true_sell,

        //        /* ABSORPTION DETECTION */
        //        CASE 
        //            WHEN price = ask_price AND ask_qty > size * 3 THEN 'Ask Absorption'
        //            WHEN price = bid_price AND bid_qty > size * 3 THEN 'Bid Absorption'
        //            ELSE 'None'
        //        END AS absorption_type,

        //        /* SPOOFING PROBABILITY (simple heuristic) */
        //        CASE
        //            WHEN (ask_qty + bid_qty) > 0 AND size = 0
        //                 AND (ask_qty > LAG(ask_qty) OVER (ORDER BY ts) * 2
        //                      OR bid_qty > LAG(bid_qty) OVER (ORDER BY ts) * 2)
        //            THEN 'Possible Spoof'
        //            ELSE 'Normal'
        //        END AS spoof_flag

        //    FROM raw_ticks
        //    WHERE instrument_name = @instrumentName
        //      AND ts BETWEEN @startUtc AND @endUtc
        //),

        //bucketed AS (
        //    SELECT
        //        (date_trunc('minute', ts_ist)
        //           - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin)) AS candle_time,

        //        ts_ist,
        //        ts,
        //        instrument_key,
        //        instrument_name,
        //        price, size, oi,
        //        price_buy, price_sell,
        //        true_buy, true_sell,
        //        order_imbalance, absorption_type, spoof_flag,
        //        true_cvd_tick,

        //        ROW_NUMBER() OVER (
        //            PARTITION BY (date_trunc('minute', ts_ist)
        //              - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))
        //            ORDER BY ts_ist
        //        ) AS rn_open,

        //        ROW_NUMBER() OVER (
        //            PARTITION BY (date_trunc('minute', ts_ist)
        //              - make_interval(mins => EXTRACT(MINUTE FROM ts_ist)::int % @aggmin))
        //            ORDER BY ts_ist DESC
        //        ) AS rn_close
        //    FROM ticks
        //),

        //candles AS (
        //    SELECT
        //        candle_time,
        //        MAX(price) FILTER (WHERE rn_open = 1)  AS open_price,
        //        MAX(price)                             AS high_price,
        //        MIN(price)                             AS low_price,
        //        MAX(price) FILTER (WHERE rn_close = 1) AS close_price,

        //        SUM(size) AS total_volume,
        //        MAX(oi) AS last_oi,

        //        /* PRICE-BASED DELTA */
        //        SUM(price_buy) AS price_buy_delta,
        //        SUM(price_sell) AS price_sell_delta,

        //        /* TRUE ORDERFLOW DELTA */
        //        SUM(true_buy) AS true_buy_delta,
        //        SUM(true_sell) AS true_sell_delta,

        //        /* HYBRID DELTA = price-based + true */
        //        SUM(true_buy + price_buy) AS hybrid_buy_delta,
        //        SUM(true_sell + price_sell) AS hybrid_sell_delta,

        //        /* ORDERFLOW FIELDS */
        //        SUM(order_imbalance) AS total_order_imbalance,
        //        COUNT(*) FILTER (WHERE absorption_type <> 'None') AS absorption_count,
        //        COUNT(*) FILTER (WHERE spoof_flag = 'Possible Spoof') AS spoof_count,

        //        MAX(true_cvd_tick) AS true_cvd_point

        //    FROM bucketed
        //    GROUP BY candle_time
        //),

        //final AS (
        //    SELECT
        //        *,
        //        LAG(close_price) OVER (ORDER BY candle_time) AS prev_close,
        //        LAG(cum_true_delta) OVER (ORDER BY candle_time) AS prev_cum_true_delta
        //    FROM (
        //        SELECT
        //            candle_time,
        //            open_price, high_price, low_price, close_price,
        //            total_volume, last_oi,

        //            /* PRICE DELTA + CVD */
        //            price_buy_delta - price_sell_delta AS price_delta,
        //            SUM(price_buy_delta - price_sell_delta)
        //                OVER (ORDER BY candle_time) AS cum_price_delta,

        //            /* TRUE DELTA + CVD */
        //            true_buy_delta - true_sell_delta AS true_delta,
        //            SUM(true_buy_delta - true_sell_delta)
        //                OVER (ORDER BY candle_time) AS cum_true_delta,

        //            /* HYBRID DELTA + CVD */
        //            hybrid_buy_delta - hybrid_sell_delta AS hybrid_delta,
        //            SUM(hybrid_buy_delta - hybrid_sell_delta)
        //                OVER (ORDER BY candle_time) AS cum_hybrid_delta,

        //            /* ORDER IMBALANCE */
        //            total_order_imbalance,
        //            SUM(total_order_imbalance)
        //                OVER (ORDER BY candle_time) AS cum_order_imbalance,

        //            /* OI CHANGE */
        //            last_oi - LAG(last_oi) OVER (ORDER BY candle_time) AS oi_change,

        //            /* PRICE CHANGE */
        //            close_price - LAG(close_price) OVER (ORDER BY candle_time) AS price_change,

        //            absorption_count,
        //            spoof_count
        //        FROM candles
        //    ) sub
        //)


        //-- RESULT SET 1: raw tick rows (for Tick Grid)
        //SELECT
        //    ts, ts_ist, instrument_key, instrument_name, price, size, bid_price, bid_qty, ask_price, ask_qty, oi,
        //    order_imbalance, true_cvd_tick, price_buy, price_sell, true_buy, true_sell, absorption_type, spoof_flag
        //FROM ticks
        //ORDER BY ts_ist;

        //-- RESULT SET 2: candle rows (OHLC + per-candle deltas)
        //SELECT
        //    candle_time, open_price, high_price, low_price, close_price, total_volume, last_oi,
        //    price_buy_delta, price_sell_delta,
        //    true_buy_delta, true_sell_delta,
        //    hybrid_buy_delta, hybrid_sell_delta,
        //    total_order_imbalance, absorption_count, spoof_count, true_cvd_point
        //FROM candles
        //ORDER BY candle_time;

        //-- RESULT SET 3: final CVD + cumulative numbers (for CVD grid)
        //SELECT
        //    candle_time,
        //    price_delta, price_cvd,
        //    true_delta, true_cvd,
        //    hybrid_delta, hybrid_cvd,
        //    total_order_imbalance, cum_order_imbalance,
        //    oi_change, price_change,
        //    absorption_count, spoof_count,
        //    cvd_divergence
        //FROM final
        //ORDER BY candle_time;

        //-- RESULT SET 4: absorption/spoof events (filtered)
        //SELECT
        //    candle_time, absorption_count, spoof_count
        //FROM final
        //WHERE absorption_count > 0 OR spoof_count > 0
        //ORDER BY candle_time;

        //-- RESULT SET 5: divergence alerts (non-neutral)
        //SELECT
        //    candle_time, cvd_divergence
        //FROM final
        //WHERE cvd_divergence <> 'Neutral'
        //ORDER BY candle_time;
        //";


        //                    using (var cmd = new NpgsqlCommand(sql, con))
        //                    {
        //                        cmd.Parameters.AddWithValue("instrumentName", symbol);
        //                        cmd.Parameters.AddWithValue("startUtc", startUtc);
        //                        cmd.Parameters.AddWithValue("endUtc", endUtc);
        //                        cmd.Parameters.AddWithValue("aggmin", aggmin);
        //                        // Note: aggmin parameter is not used in the query - you might want to remove it
        //                        // or add aggregation logic if needed
        //                        using (var reader = await cmd.ExecuteReaderAsync())
        //                        {
        //                            // 1st result → ticks
        //                            var ticks = new DataTable();
        //                            ticks.Load(reader);

        //                            // Move to next SELECT
        //                            await reader.NextResultAsync();

        //                            // 2nd result → candles
        //                            var candles = new DataTable();
        //                            candles.Load(reader);

        //                            await reader.NextResultAsync();

        //                            // 3rd result → final cvd
        //                            var final = new DataTable();
        //                            final.Load(reader);

        //                            await reader.NextResultAsync();

        //                            // 4th → absorption
        //                            var absorption = new DataTable();
        //                            absorption.Load(reader);

        //                            await reader.NextResultAsync();

        //                            // 5th → divergences
        //                            var divergence = new DataTable();
        //                            divergence.Load(reader);

        //                            // Now bind each table to separate datagrids:
        //                            //TickGrid.ItemsSource = ticks.DefaultView;
        //                            //CandleGrid.ItemsSource = candles.DefaultView;
        //                            //CVDGrid.ItemsSource = final.DefaultView;
        //                            //AbsorptionGrid.ItemsSource = absorption.DefaultView;
        //                            //DivergenceGrid.ItemsSource = divergence.DefaultView;
        //                        }

        //                        //var dt = new DataTable();
        //                        //using (var reader = await cmd.ExecuteReaderAsync())
        //                        //{
        //                        //    dt.Load(reader);
        //                        //}

        //                        DataGridCVD.ItemsSource = dt.DefaultView;
        //                        MessageBox.Show("Done");
        //                    }
        //                }
        //            }
        //            catch (Exception ex)
        //            {
        //                MessageBox.Show($"Error: {ex.Message}");
        //            }
        //        }
    }

}
public static class DictionaryExtensions
{
    public static TValue GetValueOrDefault<TKey, TValue>(
        this IDictionary<TKey, TValue> d,
        TKey key,
        TValue def)
    {
        if (d.TryGetValue(key, out var v))
            return v;

        return def;
    }
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


