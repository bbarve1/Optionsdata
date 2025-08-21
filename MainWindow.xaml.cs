using Com.Upstox.Marketdatafeederv3udapi.Rpc.Proto;
using MongoDB.Bson;
using MongoDB.Driver;
using RestSharp;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Data;
using System.Windows.Threading;
using Type = Com.Upstox.Marketdatafeederv3udapi.Rpc.Proto.Type;
// ===== Replace with your actual Upstox proto namespaces/types =====
//using FeedResponse = Your.Upstox.Proto.FeedResponse;  // <-- CHANGE
//using Type = Your.Upstox.Proto.Type;                  // <-- CHANGE
// ================================================================

namespace CVD
{
    public partial class MainWindow : Window, INotifyPropertyChanged
    {
        private readonly ConcurrentQueue<SignalRow> _pendingSignals = new ConcurrentQueue<SignalRow>();

        public ObservableCollection<SignalRow> Signals { get; private set; }
        public ICollectionView SignalsView { get; private set; }
        private ClientWebSocket webSocket;

        private readonly Dictionary<string, string> _instrumentNameMap;
        private readonly OrderFlowEngine _engine;
        private readonly List<string> instruments = new List<string>();

        private HashSet<string> equitySet = new HashSet<string>();
        private HashSet<string> fnoSet = new HashSet<string>();

        private CancellationTokenSource _cts;
        public string accessToken;
        public event PropertyChangedEventHandler PropertyChanged;

       
        private void OnPropertyChanged([CallerMemberName] string name = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
        }
        //private void OnPropertyChanged([CallerMemberName] string name = null)
        //{
        //    var handler = PropertyChanged;
        //    if (handler != null) handler(this, new PropertyChangedEventArgs(name));
        //}

        public MainWindow()
        {
            InitializeComponent();

            //_instrumentNameMap = new Dictionary<string, string>();
            //Getsymbollist();
            //Signals = new ObservableCollection<SignalRow>();
            //SignalsView = CollectionViewSource.GetDefaultView(Signals);

            //this.DataContext = this;  // <- binds DataGrid to this window's properties



            //_engine = new OrderFlowEngine(_instrumentNameMap);
            
            //_engine.OnSignal = (symbolName, t, price, s, reason, sigType) =>
            //{
            //    if (s == null) return;

            //    bool important = sigType.Contains("ICEBERG") || Math.Abs(s.Cvd1m.Sum) >= 100;
            //    if (!important) return;

            //    _pendingSignals.Enqueue(new SignalRow
            //    {
            //        Time = t,
            //        Symbol = symbolName,
            //        Ltp = price,
            //        VWAP = s.VWAP ?? double.NaN,
            //        Cvd1m = (long)Math.Round(s.Cvd1m?.Sum ?? 0),
            //        Vol1m = (long)Math.Round(s.Vol1m?.Sum ?? 0),
            //        RecentHigh = s.RecentHigh,
            //        RecentLow = s.RecentLow,
            //        SignalType = sigType,
            //        Reason = reason
            //    });
            //};
            //StartSignalUpdater();

            _instrumentNameMap = new Dictionary<string, string>();
            Getsymbollist();
            Signals = new ObservableCollection<SignalRow>();
            SignalsView = CollectionViewSource.GetDefaultView(Signals);
            this.DataContext = this;

            _engine = new OrderFlowEngine(_instrumentNameMap);

            _engine.OnSignal = (symbolName, t, price, s, reason, sigType) =>
            {
                if (s == null) return;

                bool important = sigType.Contains("ICEBERG") || Math.Abs(s.Cvd1m.Sum) >= 100;
                if (!important) return;

                _pendingSignals.Enqueue(new SignalRow
                {
                    Time = t,
                    Symbol = symbolName,
                    Ltp = price,
                    VWAP = s.VWAP ?? double.NaN,
                    Cvd1m = (long)Math.Round(s.Cvd1m?.Sum ?? 0),
                    Vol1m = (long)Math.Round(s.Vol1m?.Sum ?? 0),
                    RecentHigh = s.RecentHigh,
                    RecentLow = s.RecentLow,
                    SignalType = sigType,
                    Reason = reason
                });
            };

            StartSignalUpdater();

        }

        private void SetupSignalsView()
        {
            SignalsView.Filter = obj =>
            {
                if (obj is SignalRow row)
                {
                    // ⚡ Only show ICEBERG or strong CVD signals
                    return row.SignalType.Contains("ICEBERG") || Math.Abs(row.Cvd1m) >= 100;
                }
                return false;
            };

            SignalsView.SortDescriptions.Clear();
            SignalsView.SortDescriptions.Add(new SortDescription(nameof(SignalRow.Cvd1m), ListSortDirection.Descending));
        }
        private void StartSignalUpdater()
        {
            var timer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(500) };
            timer.Tick += (s, e) =>
            {
                while (_pendingSignals.TryDequeue(out var row))
                {
                    Signals.Insert(0, row); // insert at top
                }
            };
            timer.Start();
        }
        private bool SignalFilter(object obj)
        {
            var row = obj as SignalRow;
            if (row == null) return false;

            var q = (SearchBox.Text ?? "").Trim().ToUpperInvariant();
            if (q.Length > 0 && row.Symbol != null && row.Symbol.ToUpperInvariant().IndexOf(q, StringComparison.Ordinal) < 0)
                return false;

            if (OnlyFNOCheck.IsChecked == true && fnoSet.Count > 0)
                return fnoSet.Contains(row.Symbol);

            return true;
        }

        // -------- Toolbar handlers --------
        private async void Start_Click(object sender, RoutedEventArgs e) 
        {
            var MyLoginresult = await FetchAccessToken();

            string UpstoxLoginStatus = MyLoginresult;

            if (UpstoxLoginStatus == "")
            {
                MessageBox.Show("Login Required");
                return;
            }
            await StartAsync(); 
        }
        private async Task<string> FetchAccessToken()
        {
            string UpstoxLoingStatus = "";
            var connectionString = "mongodb://localhost:27017"; // Replace with your MongoDB URI
            var client1 = new MongoClient(connectionString);

            var database = client1.GetDatabase("StockDB");
            var collection = database.GetCollection<BsonDocument>("GetAccessToken");

            var allDocuments = await collection.Find(new BsonDocument()).ToListAsync();

            // 4. Loop through results
            foreach (var doc in allDocuments)
            {
                accessToken = doc["Token"].AsString;
                // Break if you only need the first one
                break;
            }
            var client = new RestClient("https://api.upstox.com/v2/");
            var request = new RestRequest("market-quote/quotes", Method.Get);
            request.AddHeader("Authorization", $"Bearer {accessToken}");
            request.AddParameter("symbol", "NSE_EQ|INE466L01038");
            RestResponse response = client.Execute(request);
            if (response.IsSuccessful)
            {
                UpstoxLoingStatus = "LoggedIn";
                

            }
            else
            {
                //UpstoxLoingStatus = "Login Required";
               

            }
            await Task.CompletedTask;
            return UpstoxLoingStatus;

        }

        private async void Stop_Click(object sender, RoutedEventArgs e)
        {
            try { if (_cts != null) _cts.Cancel(); } catch { }
            if (webSocket != null && webSocket.State == WebSocketState.Open)
                await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "User Stop", CancellationToken.None);
        }
        private void ClearSignals_Click(object sender, RoutedEventArgs e) { Signals.Clear(); }
        private async void ExportCsv_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var path = System.IO.Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.Desktop),
                    $"signals_{DateTime.Now:yyyyMMdd_HHmmss}.csv");

                var lines = new List<string>();

                // Add CSV header with proper escaping
                lines.Add("\"Time\",\"Symbol\",\"LTP\",\"VWAP\",\"CVD1m\",\"Vol1m\",\"RecentHigh\",\"RecentLow\",\"Signal\",\"Reason\",\"SignalDirection\"");

                foreach (var s in Signals.Reverse())
                {
                    // Proper CSV escaping: replace quotes with double quotes and wrap in quotes
                    var reason = (s.Reason ?? "").Replace("\"", "\"\"");
                    var symbol = (s.Symbol ?? "").Replace("\"", "\"\"");
                    var signalType = (s.SignalType ?? "").Replace("\"", "\"\"");

                    lines.Add($"\"{s.Time:HH:mm:ss}\",\"{symbol}\",{s.Ltp:F2},{s.VWAP:F2},{s.Cvd1m},{s.Vol1m},{s.RecentHigh:F2},{s.RecentLow:F2},\"{signalType}\",\"{reason}\",\"{s.SignalDirection}\"");
                }

                // Actually write the file (uncomment and fix this line)
                using (var writer = new StreamWriter(path, false, Encoding.UTF8))
                {
                    foreach (var line in lines)
                    {
                        await writer.WriteLineAsync(line);
                    }
                }

                OutputTextBox.AppendText($"Exported: {path}\n");

                // Optional: Open the file after export
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = path,
                    UseShellExecute = true
                });
            }
            catch (Exception ex)
            {
                OutputTextBox.AppendText($"Export error: {ex.Message}\n");
            }
        }// =================== Your Startup ===================
        // NOTE: you already have 'Start
        // Async' skeleton; this merges engine + UI.

        private async Task Getsymbollist()
        {
            var dataFetcher = new InstrumentsData(accessToken); // <-- your class/field

            var result = await dataFetcher.GetInstrumentsAsync();
            // tuple deconstruction supported since C# 7:
            var equityList = result.Item1;
            var fnoListLocal = result.Item2;
            var niftyCE = result.Item3;
            var niftyPE = result.Item4;
            var bankniftyCE = result.Item5;
            var bankniftyPEt = result.Item6;
            var instrumentNameMap = result.Item7;




            // Subscribe list (example: NIFTY options)
            
            //foreach (var i in niftyCE) if (!string.IsNullOrEmpty(i.instrument_key)) instruments.Add(i.instrument_key);
            //foreach (var i in niftyPE) if (!string.IsNullOrEmpty(i.instrument_key)) instruments.Add(i.instrument_key);

            foreach (var i in equityList) if (!string.IsNullOrEmpty(i.instrument_key)) instruments.Add(i.instrument_key);
            //foreach (var i in fnoListLocal) if (!string.IsNullOrEmpty(i.instrument_key)) instruments.Add(i.instrument_key);


            // Sets for filter
            equitySet = new HashSet<string>(equityList.Select(x => x.instrument_key));
            fnoSet = new HashSet<string>(fnoListLocal.Select(x => x.instrument_key));
            foreach (var i in niftyCE) fnoSet.Add(i.instrument_key);
            foreach (var i in niftyPE) fnoSet.Add(i.instrument_key);
            //Dictionary<string, string> _instrumentNameMap = new Dictionary<string, string>();
            foreach (var symbol in instruments)
            {
                // First check in equityList
                var eqItem = equityList.FirstOrDefault(x => x.instrument_key == symbol);
                if (eqItem != null)
                {
                    _instrumentNameMap[symbol] = eqItem.trading_symbol;
                    continue; // found in equity, skip FNO check
                }

                // Then check in FNO list
                var fnoItem = niftyPE.FirstOrDefault(x => x.instrument_key == symbol);
                if (fnoItem != null)
                {
                    _instrumentNameMap[symbol] = fnoItem.trading_symbol;
                }
            }
            await Task.CompletedTask;
        }
        private async Task StartAsync()
        {
            _cts = new CancellationTokenSource();

            
            try
            {
                webSocket = new ClientWebSocket();
                webSocket.Options.KeepAliveInterval = TimeSpan.FromSeconds(30);

                var wsUrl = await GetWebSocketUrlAsync(); // <-- your function
                if (wsUrl == null) return;

                await webSocket.ConnectAsync(new Uri(wsUrl), _cts.Token);

                await SendSubscriptionAsync(instruments); // <-- your function

               

                

              
                OutputTextBox.AppendText(string.Format("Connected & subscribed to {0} instruments.\n", instruments.Count));

                await ReceiveMessages();
            }
            catch (Exception ex)
            {
                Dispatcher.Invoke(delegate { OutputTextBox.AppendText("Error in StartAsync: " + ex.Message + "\n"); });
            }
        }
        private async Task<string> GetWebSocketUrlAsync()
        {
            var client = new RestClient("https://api.upstox.com/v3");
            var request = new RestRequest("/feed/market-data-feed/authorize", Method.Get);
            request.AddHeader("Authorization", $"Bearer {accessToken}");
            request.AddHeader("Accept", "application/json");
            request.AddHeader("Api-Version", "3.0");

            var response = await client.ExecuteAsync(request);
            Dispatcher.Invoke(() => OutputTextBox.Text += $"Authorize response: {response.Content}\n");

            if (response.IsSuccessful)
            {
                var json = JsonDocument.Parse(response.Content);
                if (json.RootElement.GetProperty("status").GetString() == "success")
                {
                    var wsUrl = json.RootElement.GetProperty("data").GetProperty("authorizedRedirectUri").GetString();
                    Dispatcher.Invoke(() => OutputTextBox.Text += $"Retrieved WebSocket URL: {wsUrl}\n");
                    return wsUrl;
                }
            }

            Dispatcher.Invoke(() => OutputTextBox.Text += $"Error retrieving WebSocket URL: {response.ErrorMessage ?? response.Content}\n");
            return null;
        }

        private async Task SendSubscriptionAsync(List<string> instrumentKeys)
        {
            try
            {
                int batchSize = 100; // adjust based on API limits
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

                    var subscriptionBytes = Encoding.UTF8.GetBytes(subscriptionJson);
                    await webSocket.SendAsync(new ArraySegment<byte>(subscriptionBytes), WebSocketMessageType.Binary, true, CancellationToken.None);

                    // Optional: avoid flooding
                    await Task.Delay(200);
                }
            }
            catch (Exception ex)
            {
                Dispatcher.Invoke(() => OutputTextBox.Text += $"Error sending subscription: {ex.Message}\n");
            }
        }


        // =================== Socket Loop ===================
        // Buffer to accumulate partial WebSocket binary frames
        private List<byte> binaryBuffer = new List<byte>();

        private async Task ReceiveMessages()
        {
            try
            {
                var buffer = new byte[1024 * 64];

                while (webSocket.State == WebSocketState.Open && !_cts.IsCancellationRequested)
                {
                    try
                    {
                        var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);

                        // ---------------------- Binary Messages ----------------------
                        if (result.MessageType == WebSocketMessageType.Binary)
                        {
                            // Add received bytes to our buffer
                            binaryBuffer.AddRange(buffer.Take(result.Count));

                            // If this is the final chunk of the message, parse it
                            if (result.EndOfMessage)
                            {
                                try
                                {
                                    using (var ms = new MemoryStream(binaryBuffer.ToArray()))
                                    {
                                        var feedResponse = FeedResponse.Parser.ParseFrom(ms);
                                        await ProcessFeedResponse(feedResponse);
                                    }
                                    
                                }
                                catch (Exception ex)
                                {
                                    Dispatcher.Invoke(() =>
                                    {
                                        OutputTextBox.AppendText("Error parsing FeedResponse: " + ex.Message + "\n");
                                    });
                                }
                                finally
                                {
                                    binaryBuffer.Clear(); // Always clear buffer after processing
                                }
                            }
                        }
                        // ---------------------- Text Messages ----------------------
                        else if (result.MessageType == WebSocketMessageType.Text)
                        {
                            var text = Encoding.UTF8.GetString(buffer, 0, result.Count);
                            Dispatcher.Invoke(() =>
                            {
                                OutputTextBox.AppendText("[Text] " + text + "\n");
                            });
                        }
                        // ---------------------- Close Messages ----------------------
                        else if (result.MessageType == WebSocketMessageType.Close)
                        {
                            Dispatcher.Invoke(() =>
                            {
                                OutputTextBox.AppendText("WebSocket closed. Status: " + result.CloseStatus + "\n");
                            });
                            break;
                        }
                    }
                    catch (WebSocketException ex)
                    {
                        Dispatcher.Invoke(() =>
                        {
                            OutputTextBox.AppendText("WebSocket error: " + ex.Message + ". State: " + webSocket.State + "\n");
                        });
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                Dispatcher.Invoke(() =>
                {
                    OutputTextBox.AppendText("Error in ReceiveMessages: " + ex.Message + "\n");
                });
            }
            finally
            {
                try
                {
                    if (webSocket != null && webSocket.State == WebSocketState.Open)
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
                }
                catch { }
                webSocket?.Dispose();
                webSocket = null;
            }
        }

        // =================== Core: Process Feed ===================
        private async Task ProcessFeedResponse(FeedResponse feedResponse)
        {
            if (feedResponse.Type == Type.LiveFeed && feedResponse.Feeds != null)
            {
                foreach (var kv in feedResponse.Feeds)
                {
                    var key = kv.Key;
                    var feed = kv.Value;

                    //if (OnlyFNOCheck.IsChecked == true && fnoSet.Count > 0 && !fnoSet.Contains(key))
                    //    continue;

                    // Try ltpc from direct feed (ltpc or firstLevelWithGreeks)
                    var ltpcObj = feed.Ltpc;

                    // If still null, try from fullFeed.marketFF
                    if (ltpcObj == null)
                        ltpcObj = feed.FullFeed?.MarketFF?.Ltpc;

                    if (ltpcObj == null)
                        return; // No price data, skip

                    double ltp = ltpcObj.Ltp;
                    long lttMs = ltpcObj.Ltt;
                    if (ltp <= 0 || lttMs == 0) return;

                    DateTime t = DateTimeOffset.FromUnixTimeMilliseconds(lttMs).LocalDateTime;
                    int ltq = (int)ltpcObj.Ltq;


                    //DateTime t = DateTimeOffset.FromUnixTimeMilliseconds(lttMs).LocalDateTime;
                    //int ltq = (int)(feed.Ltpc?.Ltq ?? 0);

                    // ---- Level-1 best bid/ask ----
                    double bestBid = 0, bestAsk = 0;
                    int bidQty = 0, askQty = 0;

                    var full = feed.FullFeed;
                    if (full?.MarketFF?.MarketLevel?.BidAskQuote?.Count >= 2)
                    {
                        var best5 = full.MarketFF.MarketLevel.BidAskQuote;
                        var b0 = best5.FirstOrDefault(q => q.BidP > 0);
                        var a0 = best5.FirstOrDefault(q => q.AskP > 0);

                        if (b0 != null && a0 != null)
                        {
                            bestBid = b0.BidP;
                            bidQty = (int)b0.BidQ;
                            bestAsk = a0.AskP;
                            askQty = (int)a0.AskQ;
                        }
                    }

                    // Fallback if no best bid/ask
                    if (bestBid <= 0 || bestAsk <= 0)
                    {
                        bestBid = ltp * 0.9995;
                        bestAsk = ltp * 1.0005;
                        int q = ltq > 0 ? ltq : 1;
                        bidQty = q;
                        askQty = q;
                    }

                    _engine.OnTick(key, t, ltp, ltq, bestBid, bidQty, bestAsk, askQty);
                }
            }

                

            await Task.CompletedTask;
        }

    }

    public class SignalRow
    {
        public DateTime Time { get; set; }
        public string Symbol { get; set; }
        public double Ltp { get; set; }
        public double VWAP { get; set; }
        public long Cvd1m { get; set; }
        public long Vol1m { get; set; }
        public double RecentHigh { get; set; }
        public double RecentLow { get; set; }
        public string SignalType { get; set; }
        public string Reason { get; set; }

        public string SignalDirection
        {
            get
            {
                if (SignalType.Contains("BUY") || SignalType.Contains("LONG") || SignalType.Contains("ACCUMULATION"))
                    return "BUY";
                if (SignalType.Contains("SELL") || SignalType.Contains("SHORT") || SignalType.Contains("DISTRIBUTION"))
                    return "SELL";
                return "NEUTRAL";
            }
        }
    }

    public enum Side { Buy, Sell, Unknown }

    public class OrderBookSnapshot
    {
        public double BestBid { get; set; }
        public int BestBidQty { get; set; }
        public double BestAsk { get; set; }
        public int BestAskQty { get; set; }
    }

    public class RollingStat
    {
        private readonly Queue<Tuple<DateTime, double>> _q = new Queue<Tuple<DateTime, double>>();
        private readonly TimeSpan _window;
        private double _sum;

        public RollingStat(TimeSpan window)
        {
            _window = window;
        }

        public void Add(DateTime t, double v)
        {
            _q.Enqueue(Tuple.Create(t, v));
            _sum += v;

            while (_q.Count > 0 && (t - _q.Peek().Item1) > _window)
            {
                _sum -= _q.Dequeue().Item2;
            }
        }

        public double Sum { get { return _sum; } }
    }

    public class InstrumentState
    {
        public string Key { get; private set; }
        public InstrumentState(string key) { Key = key; }

        public double? LastPrice { get; set; }
        public int? LastQty { get; set; }
        public DateTime? LastTradeTime { get; set; }
        public OrderBookSnapshot Book { get; set; } = new OrderBookSnapshot();

        public long Cvd { get; set; }
        public RollingStat Cvd1m { get; private set; } = new RollingStat(TimeSpan.FromMinutes(1));
        public RollingStat Vol1m { get; private set; } = new RollingStat(TimeSpan.FromMinutes(1));

        private double _cumPV, _cumVol;
        public double? VWAP { get { return _cumVol > 0 ? _cumPV / _cumVol : (double?)null; } }

        private readonly Queue<Tuple<DateTime, double>> _price1m = new Queue<Tuple<DateTime, double>>();
        public double RecentHigh { get; private set; }
        public double RecentLow { get; private set; }

        public int LastBidQty { get; set; }
        public int LastAskQty { get; set; }
        public int PullEvents { get; set; }
        public int StackEvents { get; set; }

        public void UpdateVWAP(DateTime t, double price, int qty)
        {
            _cumPV += price * qty;
            _cumVol += qty;
            Vol1m.Add(t, qty);
        }

        public void UpdatePriceMicro(DateTime t, double price)
        {
            _price1m.Enqueue(Tuple.Create(t, price));
            while (_price1m.Count > 0 && (t - _price1m.Peek().Item1) > TimeSpan.FromMinutes(1))
                _price1m.Dequeue();

            if (_price1m.Count > 0)
            {
                double hi = double.MinValue, lo = double.MaxValue;
                foreach (var tp in _price1m)
                {
                    if (tp.Item2 > hi) hi = tp.Item2;
                    if (tp.Item2 < lo) lo = tp.Item2;
                }
                RecentHigh = hi;
                RecentLow = lo;
            }
            else
            {
                RecentHigh = price;
                RecentLow = price;
            }
        }
    }

    public class OrderFlowEngine
    {
        private readonly ConcurrentDictionary<string, InstrumentState> _state =
            new ConcurrentDictionary<string, InstrumentState>();

        private readonly Dictionary<string, string> _instrumentNameMap;
        private readonly Dictionary<string, double> _lastCvd = new Dictionary<string, double>();

        // Threshold configuration
        private const double SpikeThresholdPercent = 0.1; // 10% of 1-min volume
        private const double MinSpikeThreshold = 50; // Minimum absolute threshold
        private const double VolumeThreshold = 1000; // Minimum volume to consider
        private const double SignificantCvdThreshold = 100; // Absolute CVD threshold for important signals

        public OrderFlowEngine(Dictionary<string, string> instrumentNameMap)
        {
            _instrumentNameMap = instrumentNameMap;
        }

        public Action<string, DateTime, double, InstrumentState, string, string> OnSignal { get; set; }

        public InstrumentState Get(string key)
        {
            return _state.GetOrAdd(key, k => new InstrumentState(k));
        }

        private static Side ClassifyAggressor(double price, OrderBookSnapshot book, double? prevMid)
        {
            if (book == null) return Side.Unknown;
            if (price >= book.BestAsk) return Side.Buy;
            if (price <= book.BestBid) return Side.Sell;

            double mid = (book.BestBid + book.BestAsk) / 2.0;
            double refMid = prevMid ?? mid;
            return price >= refMid ? Side.Buy : Side.Sell;
        }

        public Tuple<bool, string> OnTick(string key, DateTime t, double ltp, int ltq,
                                  double bestBid, int bidQty, double bestAsk, int askQty)
        {
            var s = Get(key);

            // keep prevMid BEFORE book update
            double prevMid = (s.Book.BestBid + s.Book.BestAsk) / 2.0;

            // -------------------- Update book --------------------
            if (s.LastBidQty != 0 && s.LastAskQty != 0)
            {
                if (askQty < (int)(s.LastAskQty * 0.6)) s.PullEvents++;
                if (bidQty > (int)(s.LastBidQty * 1.4)) s.StackEvents++;
            }
            s.Book.BestBid = bestBid; s.Book.BestBidQty = bidQty;
            s.Book.BestAsk = bestAsk; s.Book.BestAskQty = askQty;
            s.LastBidQty = bidQty; s.LastAskQty = askQty;

            // -------------------- Trade detection --------------------
            bool hasTrade =
                (!s.LastTradeTime.HasValue || t > s.LastTradeTime.Value) &&
                ltq > 0 &&
                (!s.LastPrice.HasValue || ltp != s.LastPrice.Value || ltq != s.LastQty);

            if (hasTrade)
            {
                var side = ClassifyAggressor(ltp, s.Book, prevMid);
                int signedVol = side == Side.Buy ? ltq : (side == Side.Sell ? -ltq : 0);

                s.Cvd += signedVol;
                s.Cvd1m.Add(t, signedVol);

                s.UpdateVWAP(t, ltp, ltq);
                s.UpdatePriceMicro(t, ltp);

                s.LastPrice = ltp;
                s.LastQty = ltq;
                s.LastTradeTime = t;
            }
            else
            {
                s.UpdatePriceMicro(t, ltp);
                s.LastPrice = ltp;
            }

            // -------------------- CVD spike detection --------------------
            bool fired = false;
            string reason = "";
            string type = "";

            double currentCvd = s.Cvd1m.Sum;
            double lastCvd = _lastCvd.TryGetValue(key, out var c) ? c : 0;
            double delta = currentCvd - lastCvd;

            // Use dynamic threshold based on volume
            double avgVol = s.Vol1m.Sum;
            double dynamicThreshold = Math.Max(MinSpikeThreshold, avgVol * SpikeThresholdPercent);

            // Only check for spikes if we have sufficient volume
            if (avgVol >= VolumeThreshold && Math.Abs(delta) >= dynamicThreshold)
            {
                fired = true;
                type = "CVD_SPIKE";
                reason = $"ΔCVD={delta:F0} (Threshold: {dynamicThreshold:F0}), TotalCVD={currentCvd:F0}, Vol1m={avgVol:F0}";
            }

            // Always update lastCvd to avoid duplicate spikes
            _lastCvd[key] = currentCvd;

            // -------------------- Other signal detection rules --------------------
            var vwap = s.VWAP;
            if (!fired && vwap.HasValue && s.Vol1m.Sum >= VolumeThreshold)
            {
                // Accumulation pattern detection
                bool nearLow = s.RecentLow > 0 && ltp <= s.RecentLow * 1.002;
                bool cvdUp = s.Cvd1m.Sum > Math.Max(SignificantCvdThreshold, s.Vol1m.Sum * 0.05);
                bool flowUp = s.StackEvents > s.PullEvents;

                if (nearLow && cvdUp && flowUp && ltp <= vwap.Value * 1.002)
                {
                    fired = true;
                    type = "ACCUMULATION_LONG";
                    reason = $"CVD1m={s.Cvd1m.Sum:F0}, Vol1m={s.Vol1m.Sum:F0}, VWAP={vwap.Value:F2}";
                }
            }

            if (!fired && s.RecentHigh > 0 && s.Vol1m.Sum >= VolumeThreshold)
            {
                // Breakout pattern detection
                bool brokeHigh = ltp > s.RecentHigh * 1.0015;
                bool cvdStrong = s.Cvd1m.Sum > Math.Max(SignificantCvdThreshold * 2, s.Vol1m.Sum * 0.08);
                bool asksPulled = s.PullEvents > s.StackEvents;

                if (brokeHigh && cvdStrong && asksPulled)
                {
                    fired = true;
                    type = "BREAKOUT_LONG";
                    reason = $"CVD1m={s.Cvd1m.Sum:F0}, Vol1m={s.Vol1m.Sum:F0}, AskPull={s.PullEvents} BidStack={s.StackEvents}";
                }
            }

            // -------------------- Extreme CVD values detection --------------------
            if (!fired && Math.Abs(s.Cvd1m.Sum) >= SignificantCvdThreshold * 5 && s.Vol1m.Sum >= VolumeThreshold * 2)
            {
                fired = true;
                type = s.Cvd1m.Sum > 0 ? "EXTREME_BUYING" : "EXTREME_SELLING";
                reason = $"Extreme CVD={s.Cvd1m.Sum:F0}, Vol1m={s.Vol1m.Sum:F0}";
            }

            // decay counters
            s.PullEvents = (int)(s.PullEvents * 0.7);
            s.StackEvents = (int)(s.StackEvents * 0.7);

            // -------------------- Emit Signal --------------------
            //if (fired && OnSignal != null)
            //{
            //    string stockName = _instrumentNameMap.TryGetValue(key, out var name) ? name : key;
            //    OnSignal(stockName, t, ltp, s, reason, type);
            //}
            if (fired && OnSignal != null)
            {
                string stockName = _instrumentNameMap.TryGetValue(key, out var name) ? name : key;
                OnSignal(stockName, t, ltp, s, reason, type);
                //Console.WriteLine($"Signal added: {stockName} {type} {reason}");
            }
            return Tuple.Create(fired, reason);
        }
    }

    //public class OrderFlowEngine
    //{
    //    private readonly ConcurrentDictionary<string, InstrumentState> _state =
    //        new ConcurrentDictionary<string, InstrumentState>();

    //    private readonly Dictionary<string, string> _instrumentNameMap;
    //    private readonly Dictionary<string, double> _lastCvd = new Dictionary<string, double>();

    //    private const double SpikeThreshold = 20000; // adjust for your liquidity

    //    public OrderFlowEngine(Dictionary<string, string> instrumentNameMap)
    //    {
    //        _instrumentNameMap = instrumentNameMap;
    //    }

    //    public Action<string, DateTime, double, InstrumentState, string, string> OnSignal { get; set; }

    //    public InstrumentState Get(string key)
    //    {
    //        return _state.GetOrAdd(key, k => new InstrumentState(k));
    //    }

    //    private static Side ClassifyAggressor(double price, OrderBookSnapshot book, double? prevMid)
    //    {
    //        if (book == null) return Side.Unknown;
    //        if (price >= book.BestAsk) return Side.Buy;
    //        if (price <= book.BestBid) return Side.Sell;

    //        double mid = (book.BestBid + book.BestAsk) / 2.0;
    //        double refMid = prevMid ?? mid;
    //        return price >= refMid ? Side.Buy : Side.Sell;
    //    }

    //    public Tuple<bool, string> OnTick(string key, DateTime t, double ltp, int ltq,
    //                              double bestBid, int bidQty, double bestAsk, int askQty)
    //    {
    //        var s = Get(key);

    //        // keep prevMid BEFORE book update
    //        double prevMid = (s.Book.BestBid + s.Book.BestAsk) / 2.0;

    //        // -------------------- Update book --------------------
    //        if (s.LastBidQty != 0 && s.LastAskQty != 0)
    //        {
    //            if (askQty < (int)(s.LastAskQty * 0.6)) s.PullEvents++;
    //            if (bidQty > (int)(s.LastBidQty * 1.4)) s.StackEvents++;
    //        }
    //        s.Book.BestBid = bestBid; s.Book.BestBidQty = bidQty;
    //        s.Book.BestAsk = bestAsk; s.Book.BestAskQty = askQty;
    //        s.LastBidQty = bidQty; s.LastAskQty = askQty;

    //        // -------------------- Trade detection --------------------
    //        bool hasTrade =
    //            (!s.LastTradeTime.HasValue || t > s.LastTradeTime.Value) &&
    //            ltq > 0 &&
    //            (!s.LastPrice.HasValue || ltp != s.LastPrice.Value || ltq != s.LastQty);

    //        if (hasTrade)
    //        {
    //            var side = ClassifyAggressor(ltp, s.Book, prevMid);
    //            int signedVol = side == Side.Buy ? ltq : (side == Side.Sell ? -ltq : 0);

    //            s.Cvd += signedVol;
    //            s.Cvd1m.Add(t, signedVol);

    //            s.UpdateVWAP(t, ltp, ltq);
    //            s.UpdatePriceMicro(t, ltp);

    //            s.LastPrice = ltp;
    //            s.LastQty = ltq;
    //            s.LastTradeTime = t;
    //        }
    //        else
    //        {
    //            s.UpdatePriceMicro(t, ltp);
    //            s.LastPrice = ltp;
    //        }

    //        // -------------------- CVD spike detection --------------------
    //        bool fired = false;
    //        string reason = "";
    //        string type = "";

            
    //        double currentCvd = s.Cvd1m.Sum;
    //        double lastCvd = _lastCvd.TryGetValue(key, out var c) ? c : 0;
    //        double delta = currentCvd - lastCvd;

    //        // Use 10% of last 1-minute volume as threshold
    //        double avgVol = s.Vol1m.Sum; // total volume in last 1 min
    //        double dynamicThreshold = Math.Max(50, avgVol * 0.1); // 10% of 1-min volume

    //        if (Math.Abs(delta) >= dynamicThreshold)
    //        {
    //            fired = true;
    //            type = "CVD_SPIKE";
    //            reason = $"ΔCVD={delta:F0}, TotalCVD={currentCvd:F0}, Vol1m={s.Vol1m.Sum:F0}, Threshold={dynamicThreshold:F0}";
    //        }

    //        _lastCvd[key] = currentCvd;
    //        // -------------------- Other rules (unchanged) --------------------
    //        var vwap = s.VWAP;
    //        if (!fired && vwap.HasValue && s.Vol1m.Sum > 0)
    //        {
    //            bool nearLow = s.RecentLow > 0 && ltp <= s.RecentLow * 1.002;
    //            bool cvdUp = s.Cvd1m.Sum > Math.Max(50.0, s.Vol1m.Sum * 0.05);
    //            bool flowUp = s.StackEvents > s.PullEvents;

    //            if (nearLow && cvdUp && flowUp && ltp <= vwap.Value * 1.002)
    //            {
    //                fired = true;
    //                type = "ACCUMULATION_LONG";
    //                reason = $"CVD1m={s.Cvd1m.Sum:F0}, Vol1m={s.Vol1m.Sum:F0}, VWAP={vwap.Value:F2}";
    //            }
    //        }

    //        if (!fired && s.RecentHigh > 0)
    //        {
    //            bool brokeHigh = ltp > s.RecentHigh * 1.0015;
    //            bool cvdStrong = s.Cvd1m.Sum > Math.Max(100.0, s.Vol1m.Sum * 0.08);
    //            bool asksPulled = s.PullEvents > s.StackEvents;

    //            if (brokeHigh && cvdStrong && asksPulled)
    //            {
    //                fired = true;
    //                type = "BREAKOUT_LONG";
    //                reason = $"CVD1m={s.Cvd1m.Sum:F0}, Vol1m={s.Vol1m.Sum:F0}, AskPull={s.PullEvents} BidStack={s.StackEvents}";
    //            }
    //        }

    //        // decay counters
    //        s.PullEvents = (int)(s.PullEvents * 0.7);
    //        s.StackEvents = (int)(s.StackEvents * 0.7);

    //        // -------------------- Emit Signal --------------------
    //        if (fired && OnSignal != null)
    //        {
    //            string stockName = _instrumentNameMap.TryGetValue(key, out var name) ? name : key;
    //            OnSignal(stockName, t, ltp, s, reason, type);
    //            //Console.WriteLine($"Signal added: {stockName} {type} {reason}");
    //        }

    //        return Tuple.Create(fired, reason);
    //    }

    //    #region old code on tick
    //    //public Tuple<bool, string> OnTick(string key, DateTime t, double ltp, int ltq,
    //    //                                  double bestBid, int bidQty, double bestAsk, int askQty)
    //    //{
    //    //    var s = Get(key);

    //    //    // update book
    //    //    if (s.LastBidQty != 0 && s.LastAskQty != 0)
    //    //    {
    //    //        if (askQty < (int)(s.LastAskQty * 0.6)) s.PullEvents++;
    //    //        if (bidQty > (int)(s.LastBidQty * 1.4)) s.StackEvents++;
    //    //    }
    //    //    s.Book.BestBid = bestBid; s.Book.BestBidQty = bidQty;
    //    //    s.Book.BestAsk = bestAsk; s.Book.BestAskQty = askQty;
    //    //    s.LastBidQty = bidQty; s.LastAskQty = askQty;

    //    //    // trade detection
    //    //    bool hasTrade = !s.LastTradeTime.HasValue || t > s.LastTradeTime.Value ||
    //    //                    !s.LastPrice.HasValue || ltp != s.LastPrice.Value ||
    //    //                    !s.LastQty.HasValue || ltq != s.LastQty.Value;

    //    //    if (hasTrade && ltq > 0)
    //    //    {
    //    //        double prevMid = (s.Book.BestBid + s.Book.BestAsk) / 2.0;
    //    //        var side = ClassifyAggressor(ltp, s.Book, prevMid);

    //    //        int signedVol = side == Side.Buy ? ltq : (side == Side.Sell ? -ltq : 0);
    //    //        s.Cvd += signedVol;
    //    //        s.Cvd1m.Add(t, signedVol);

    //    //        s.UpdateVWAP(t, ltp, ltq);
    //    //        s.UpdatePriceMicro(t, ltp);

    //    //        s.LastPrice = ltp;
    //    //        s.LastQty = ltq;
    //    //        s.LastTradeTime = t;
    //    //    }
    //    //    else
    //    //    {
    //    //        s.UpdatePriceMicro(t, ltp);
    //    //        s.LastPrice = ltp;
    //    //    }

    //    //    bool fired = false;
    //    //    string reason = "";
    //    //    string type = "";

    //    //    // -------------------- Built-in CVD spike detection --------------------
    //    //    double currentCvd = s.Cvd1m.Sum;
    //    //    double lastCvd = _lastCvd.TryGetValue(key, out var c) ? c : 0;
    //    //    double delta = currentCvd - lastCvd;

    //    //    if (Math.Abs(delta) >= SpikeThreshold)
    //    //    {
    //    //        fired = true;
    //    //        type = "CVD_SPIKE";
    //    //        reason = $"ΔCVD={delta:F0}, TotalCVD={currentCvd:F0}, Vol1m={s.Vol1m.Sum:F0}";
    //    //    }

    //    //    _lastCvd[key] = currentCvd;

    //    //    // -------------------- Other existing rules --------------------
    //    //    var vwap = s.VWAP;
    //    //    if (!fired && vwap.HasValue && s.Vol1m.Sum > 0)
    //    //    {
    //    //        bool nearLow = s.RecentLow > 0 && ltp <= s.RecentLow * 1.002;
    //    //        bool cvdUp = s.Cvd1m.Sum > Math.Max(50.0, s.Vol1m.Sum * 0.05);
    //    //        bool flowUp = s.StackEvents > s.PullEvents;

    //    //        if (nearLow && cvdUp && flowUp && ltp <= vwap.Value * 1.002)
    //    //        {
    //    //            fired = true;
    //    //            type = "ACCUMULATION_LONG";
    //    //            reason = $"CVD1m={s.Cvd1m.Sum:F0}, Vol1m={s.Vol1m.Sum:F0}, VWAP={vwap.Value:F2}";
    //    //        }
    //    //    }

    //    //    if (!fired && s.RecentHigh > 0)
    //    //    {
    //    //        bool brokeHigh = ltp > s.RecentHigh * 1.0015;
    //    //        bool cvdStrong = s.Cvd1m.Sum > Math.Max(100.0, s.Vol1m.Sum * 0.08);
    //    //        bool asksPulled = s.PullEvents > s.StackEvents;

    //    //        if (brokeHigh && cvdStrong && asksPulled)
    //    //        {
    //    //            fired = true;
    //    //            type = "BREAKOUT_LONG";
    //    //            reason = $"CVD1m={s.Cvd1m.Sum:F0}, Vol1m={s.Vol1m.Sum:F0}, AskPull>{s.PullEvents} BidStack={s.StackEvents}";
    //    //        }
    //    //    }

    //    //    // decay counters
    //    //    s.PullEvents = (int)(s.PullEvents * 0.7);
    //    //    s.StackEvents = (int)(s.StackEvents * 0.7);

    //    //    // -------------------- Emit Signal --------------------
    //    //    if (fired && OnSignal != null)
    //    //    {
    //    //        string stockName = _instrumentNameMap.TryGetValue(key, out var name) ? name : key;
    //    //        OnSignal(stockName, t, ltp, s, reason, type);
    //    //    }

    //    //    return Tuple.Create(fired, reason);
    //    //}
    //    #endregion

    //}


    //public class OrderFlowEngine
    //{
    //    private readonly ConcurrentDictionary<string, InstrumentState> _state =
    //        new ConcurrentDictionary<string, InstrumentState>();
    //    private readonly Dictionary<string, string> _instrumentNameMap;

    //    public OrderFlowEngine(Dictionary<string, string> instrumentNameMap)
    //    {
    //        _instrumentNameMap = instrumentNameMap;
    //    }

    //    public InstrumentState Get(string key)
    //    {
    //        return _state.GetOrAdd(key, k => new InstrumentState(k));
    //    }

    //    public Action<string, DateTime, double, InstrumentState, string, string> OnSignal { get; set; }

    //    private static Side ClassifyAggressor(double price, OrderBookSnapshot book, double? prevMid)
    //    {
    //        if (book == null) return Side.Unknown;
    //        if (price >= book.BestAsk) return Side.Buy;
    //        if (price <= book.BestBid) return Side.Sell;

    //        double mid = (book.BestBid + book.BestAsk) / 2.0;
    //        double refMid = prevMid.HasValue ? prevMid.Value : mid;
    //        return price >= refMid ? Side.Buy : Side.Sell;
    //    }

    //    public Tuple<bool, string> OnTick(string key, DateTime t, double ltp, int ltq,
    //                                      double bestBid, int bidQty, double bestAsk, int askQty)
    //    {
    //        var s = Get(key);

    //        // Pull/Stack heuristic
    //        if (s.LastBidQty != 0 && s.LastAskQty != 0)
    //        {
    //            if (askQty < (int)(s.LastAskQty * 0.6)) s.PullEvents++;
    //            if (bidQty > (int)(s.LastBidQty * 1.4)) s.StackEvents++;
    //        }
    //        s.Book.BestBid = bestBid; s.Book.BestBidQty = bidQty;
    //        s.Book.BestAsk = bestAsk; s.Book.BestAskQty = askQty;
    //        s.LastBidQty = bidQty; s.LastAskQty = askQty;

    //        bool hasTrade = !s.LastTradeTime.HasValue || t > s.LastTradeTime.Value ||
    //                        !s.LastPrice.HasValue || ltp != s.LastPrice.Value ||
    //                        !s.LastQty.HasValue || ltq != s.LastQty.Value;

    //        if (hasTrade && ltq > 0)
    //        {
    //            double prevMid = (s.Book.BestBid + s.Book.BestAsk) / 2.0;
    //            var side = ClassifyAggressor(ltp, s.Book, prevMid);

    //            int signedVol = side == Side.Buy ? ltq : (side == Side.Sell ? -ltq : 0);
    //            s.Cvd += signedVol;
    //            s.Cvd1m.Add(t, signedVol);

    //            s.UpdateVWAP(t, ltp, ltq);
    //            s.UpdatePriceMicro(t, ltp);

    //            s.LastPrice = ltp;
    //            s.LastQty = ltq;
    //            s.LastTradeTime = t;
    //        }
    //        else
    //        {
    //            s.UpdatePriceMicro(t, ltp);
    //            s.LastPrice = ltp;
    //        }

    //        bool fired = false;
    //        string reason = "";
    //        string type = "";

    //        var vwap = s.VWAP;
    //        if (vwap.HasValue && s.Vol1m.Sum > 0)
    //        {
    //            bool nearLow = s.RecentLow > 0 && ltp <= s.RecentLow * 1.002; // within 0.2%
    //            bool cvdUp = s.Cvd1m.Sum > Math.Max(50.0, s.Vol1m.Sum * 0.05);
    //            bool flowUp = s.StackEvents > s.PullEvents;

    //            if (nearLow && cvdUp && flowUp && ltp <= vwap.Value * 1.002)
    //            {
    //                fired = true;
    //                type = "ACCUMULATION_LONG";
    //                reason = string.Format("CVD1m={0:F0}, Vol1m={1:F0}, VWAP={2:F2}, Pull/Stack={3}/{4}",
    //                    s.Cvd1m.Sum, s.Vol1m.Sum, vwap.Value, s.PullEvents, s.StackEvents);
    //            }
    //        }

    //        if (!fired && s.RecentHigh > 0)
    //        {
    //            bool brokeHigh = ltp > s.RecentHigh * 1.0015;
    //            bool cvdStrong = s.Cvd1m.Sum > Math.Max(100.0, s.Vol1m.Sum * 0.08);
    //            bool asksPulled = s.PullEvents > s.StackEvents;

    //            if (brokeHigh && cvdStrong && asksPulled)
    //            {
    //                fired = true;
    //                type = "BREAKOUT_LONG";
    //                reason = string.Format("CVD1m={0:F0}, Vol1m={1:F0}, AskPull>{2} BidStack={3}",
    //                    s.Cvd1m.Sum, s.Vol1m.Sum, s.PullEvents, s.StackEvents);
    //            }
    //        }

    //        // decay counters
    //        s.PullEvents = (int)(s.PullEvents * 0.7);
    //        s.StackEvents = (int)(s.StackEvents * 0.7);


    //        if (fired && OnSignal != null)
    //        {
    //            string stockName = _instrumentNameMap.ContainsKey(key) ? _instrumentNameMap[key] : key;
    //            OnSignal(stockName, t, ltp, s, reason, type);
    //        }

    //        if (fired && OnSignal != null)
    //            OnSignal(key, t, ltp, s, reason, type);

    //        return Tuple.Create(fired, reason);
    //    }
    //}


}
