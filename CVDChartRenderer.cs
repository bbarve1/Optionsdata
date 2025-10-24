using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Shapes;

namespace CVD
{
    public class TimeframeChartSystem
    {
        private Canvas _chartCanvas;
        private TimeframeChartRenderer _chartRenderer;
        private string _currentSymbol = "ALL";
        private int _currentTimeframe = 5;

        public ComboBox SymbolComboBox { get; set; }
        public ComboBox TimeframeComboBox { get; set; }
        public Button ClearChartBtn { get; set; }
        public TextBlock StatusText { get; set; }
        private Dictionary<string, string> _instrumentNameMap;

        // Store data for ALL symbols and ALL timeframes
        private Dictionary<string, Dictionary<int, SymbolTimeframeData>> _allSymbolData =
            new Dictionary<string, Dictionary<int, SymbolTimeframeData>>();

        public TimeframeChartSystem(Canvas chartCanvas, Dictionary<string, string> instrumentMap)
        {
            _chartCanvas = chartCanvas;
            _chartRenderer = new TimeframeChartRenderer(_chartCanvas);
            _instrumentNameMap = instrumentMap;
        }

        public void InitializeControls()
        {
            // Initialize Symbol ComboBox
            if (SymbolComboBox != null)
            {
                SymbolComboBox.Items.Clear();
                SymbolComboBox.Items.Add("ALL");

                foreach (var symbol in _instrumentNameMap.Values.Distinct())
                {
                    SymbolComboBox.Items.Add(symbol);
                    // Initialize data storage for each symbol
                    _allSymbolData[symbol] = new Dictionary<int, SymbolTimeframeData>();
                }

                // Initialize data storage for "ALL"
                _allSymbolData["ALL"] = new Dictionary<int, SymbolTimeframeData>();

                SymbolComboBox.SelectedIndex = 0;
                SymbolComboBox.SelectionChanged += (s, e) =>
                {
                    if (SymbolComboBox.SelectedItem != null)
                    {
                        _currentSymbol = SymbolComboBox.SelectedItem.ToString();
                        SwitchToSymbol(_currentSymbol);
                    }
                };
            }

            // Initialize Timeframe ComboBox
            if (TimeframeComboBox != null)
            {
                TimeframeComboBox.Items.Clear();
                var timeframes = new[] { 1, 2, 3, 5, 10, 15, 30, 60 };
                foreach (var tf in timeframes)
                {
                    TimeframeComboBox.Items.Add($"{tf} Min");
                }
                TimeframeComboBox.SelectedIndex = 3;
                TimeframeComboBox.SelectionChanged += (s, e) =>
                {
                    if (TimeframeComboBox.SelectedItem != null)
                    {
                        var selected = TimeframeComboBox.SelectedItem.ToString();
                        _currentTimeframe = int.Parse(selected.Split(' ')[0]);
                        SwitchToTimeframe(_currentTimeframe);
                    }
                };
            }

            if (ClearChartBtn != null)
            {
                ClearChartBtn.Click += (s, e) => ClearAllData();
            }

            // Initialize with current symbol/timeframe
            SwitchToSymbol(_currentSymbol);
            SwitchToTimeframe(_currentTimeframe);
        }

        private void SwitchToSymbol(string symbol)
        {
            // Ensure data structure exists for this symbol
            if (!_allSymbolData.ContainsKey(symbol))
            {
                _allSymbolData[symbol] = new Dictionary<int, SymbolTimeframeData>();
            }

            // Switch chart renderer to use this symbol's data
            var timeframeData = GetOrCreateTimeframeData(symbol, _currentTimeframe);
            _chartRenderer.SetData(timeframeData);

            UpdateStatus($"Symbol: {symbol} | Timeframe: {_currentTimeframe}min");
            _chartRenderer.RenderChart();
        }

        private void SwitchToTimeframe(int timeframe)
        {
            // Ensure data structure exists for current symbol with this timeframe
            var timeframeData = GetOrCreateTimeframeData(_currentSymbol, timeframe);
            _chartRenderer.SetData(timeframeData);
            _chartRenderer.SetTimeframe(timeframe);

            UpdateStatus($"Symbol: {_currentSymbol} | Timeframe: {timeframe}min");
            _chartRenderer.RenderChart();
        }

        private SymbolTimeframeData GetOrCreateTimeframeData(string symbol, int timeframe)
        {
            if (!_allSymbolData[symbol].ContainsKey(timeframe))
            {
                _allSymbolData[symbol][timeframe] = new SymbolTimeframeData();
            }
            return _allSymbolData[symbol][timeframe];
        }

        public void ProcessMarketData(string symbol, double price, long bidQty, long askQty, long volume, DateTime timestamp)
        {
            // Process data for ALL symbols first
            ProcessDataForSymbol("ALL", price, bidQty, askQty, volume, timestamp);

            // Process data for the specific symbol
            ProcessDataForSymbol(symbol, price, bidQty, askQty, volume, timestamp);

            // If current symbol is "ALL" or matches this symbol, update the chart
            if (_currentSymbol == "ALL" || _currentSymbol == symbol)
            {
                var timeframeData = GetOrCreateTimeframeData(_currentSymbol, _currentTimeframe);
                _chartRenderer.ProcessTickData(price, bidQty, askQty, volume, timestamp);

                if (_chartRenderer.ShouldRefreshChart())
                {
                    Application.Current.Dispatcher.Invoke(() =>
                    {
                        _chartRenderer.RenderChart();
                    });
                }

                UpdateStatus($"{_currentSymbol} | {_currentTimeframe}min | Price: {price:F2} | CVD: {_chartRenderer.GetCurrentCVD():N0}");
            }
        }

        private void ProcessDataForSymbol(string symbol, double price, long bidQty, long askQty, long volume, DateTime timestamp)
        {
            if (!_allSymbolData.ContainsKey(symbol)) return;

            // Process for all timeframes for this symbol
            foreach (var timeframe in _allSymbolData[symbol].Keys.ToList())
            {
                var data = _allSymbolData[symbol][timeframe];
                data.ProcessTick(price, bidQty, askQty, volume, timestamp, timeframe);
            }
        }

        private void UpdateStatus(string message)
        {
            StatusText?.Dispatcher.Invoke(() =>
            {
                StatusText.Text = message;
            });
        }

        private void ClearAllData()
        {
            _allSymbolData.Clear();

            // Reinitialize data structures
            _allSymbolData["ALL"] = new Dictionary<int, SymbolTimeframeData>();
            foreach (var symbol in _instrumentNameMap.Values.Distinct())
            {
                _allSymbolData[symbol] = new Dictionary<int, SymbolTimeframeData>();
            }

            _chartRenderer.ClearData();
            SwitchToSymbol(_currentSymbol);
        }
    }

    public class SymbolTimeframeData
    {
        public List<TimeframeCandle> Candles { get; private set; } = new List<TimeframeCandle>();
        public TimeframeCandle CurrentCandle { get; private set; }
        public DateTime CurrentCandleTime { get; private set; }
        public bool ShouldRefresh { get; set; }

        public void ProcessTick(double price, long bidQty, long askQty, long volume, DateTime timestamp, int timeframeMinutes)
        {
            DateTime candleTime = RoundToTimeframe(timestamp, timeframeMinutes);

            if (CurrentCandle == null || candleTime != CurrentCandleTime)
            {
                if (CurrentCandle != null)
                {
                    Candles.Add(CurrentCandle);
                    ShouldRefresh = true;

                    // Keep only recent candles (optional - you can remove this limit)
                    //if (Candles.Count > 1000) // Increased limit for historical data
                    //    Candles.RemoveAt(0);
                }

                CurrentCandleTime = candleTime;
                CurrentCandle = new TimeframeCandle
                {
                    Timestamp = candleTime,
                    Open = price,
                    High = price,
                    Low = price,
                    Close = price,
                    Volume = volume,
                    CVD = bidQty - askQty
                };
            }
            else
            {
                CurrentCandle.High = Math.Max(CurrentCandle.High, price);
                CurrentCandle.Low = Math.Min(CurrentCandle.Low, price);
                CurrentCandle.Close = price;
                CurrentCandle.Volume += volume;
                CurrentCandle.CVD += (bidQty - askQty);
            }
        }

        private DateTime RoundToTimeframe(DateTime timestamp, int timeframeMinutes)
        {
            var totalMinutes = (timestamp.Hour * 60) + timestamp.Minute;
            var roundedMinutes = (totalMinutes / timeframeMinutes) * timeframeMinutes;

            return new DateTime(timestamp.Year, timestamp.Month, timestamp.Day,
                              roundedMinutes / 60, roundedMinutes % 60, 0);
        }

        public List<TimeframeCandle> GetDisplayCandles()
        {
            var displayCandles = new List<TimeframeCandle>(Candles);
            if (CurrentCandle != null)
            {
                displayCandles.Add(CurrentCandle);
            }
            return displayCandles;
        }
    }

    public class TimeframeChartRenderer
    {
        private Canvas _canvas;
        private SymbolTimeframeData _currentData;
        private int _timeframeMinutes = 5;

        // Colors and brushes remain the same...
        private Brush _bullishBrush = Brushes.LimeGreen;
        private Brush _bearishBrush = Brushes.Red;
        private Brush _cvdPositiveBrush = Brushes.Cyan;
        private Brush _cvdNegativeBrush = Brushes.Magenta;
        private Brush _gridBrush = new SolidColorBrush(Color.FromArgb(255, 60, 60, 60));
        private Brush _textBrush = Brushes.White;

        public TimeframeChartRenderer(Canvas canvas)
        {
            _canvas = canvas;
        }

        public void SetData(SymbolTimeframeData data)
        {
            _currentData = data;
        }

        public void SetTimeframe(int minutes)
        {
            _timeframeMinutes = minutes;
        }

        public void ProcessTickData(double price, long bidQty, long askQty, long volume, DateTime timestamp)
        {
            if (_currentData != null)
            {
                _currentData.ProcessTick(price, bidQty, askQty, volume, timestamp, _timeframeMinutes);
            }
        }

        public bool ShouldRefreshChart()
        {
            if (_currentData?.ShouldRefresh == true)
            {
                _currentData.ShouldRefresh = false;
                return true;
            }
            return false;
        }

        public double GetCurrentCVD()
        {
            return _currentData?.CurrentCandle?.CVD ?? 0;
        }

        public void RenderChart()
        {
            if (_canvas == null || _currentData == null) return;

            _canvas.Children.Clear();

            var displayCandles = _currentData.GetDisplayCandles();
            if (displayCandles.Count == 0) return;

            DrawGrid();
            RenderPriceCandles(displayCandles);
            RenderCVDHistogram(displayCandles);
            RenderTimeLabels(displayCandles);
        }

        // The rendering methods (DrawGrid, RenderPriceCandles, RenderCVDHistogram, RenderTimeLabels, AddText)
        // remain exactly the same as in your original code...
        private void DrawGrid()
        {
            double width = _canvas.ActualWidth;
            double height = _canvas.ActualHeight;
            double upperHeight = height * 0.7;
            double lowerHeight = height * 0.3;

            // Vertical grid lines
            for (int i = 1; i < 10; i++)
            {
                double x = width * i / 10;
                var line = new Line
                {
                    X1 = x,
                    X2 = x,
                    Y1 = 0,
                    Y2 = height,
                    Stroke = _gridBrush,
                    StrokeThickness = 0.5,
                    Opacity = 0.3
                };
                _canvas.Children.Add(line);
            }

            // Horizontal grid - Price panel
            for (int i = 1; i < 5; i++)
            {
                double y = upperHeight * i / 5;
                var line = new Line
                {
                    X1 = 0,
                    X2 = width,
                    Y1 = y,
                    Y2 = y,
                    Stroke = _gridBrush,
                    StrokeThickness = 0.5,
                    Opacity = 0.3
                };
                _canvas.Children.Add(line);
            }

            // Horizontal grid - CVD panel
            for (int i = 1; i < 3; i++)
            {
                double y = upperHeight + (lowerHeight * i / 3);
                var line = new Line
                {
                    X1 = 0,
                    X2 = width,
                    Y1 = y,
                    Y2 = y,
                    Stroke = _gridBrush,
                    StrokeThickness = 0.5,
                    Opacity = 0.3
                };
                _canvas.Children.Add(line);
            }
        }

        private void RenderPriceCandles(List<TimeframeCandle> candles)
        {
            if (candles.Count == 0) return;

            double width = _canvas.ActualWidth;
            double height = _canvas.ActualHeight;
            double upperHeight = height * 0.7;

            // Calculate price range
            double minPrice = candles.Min(c => c.Low);
            double maxPrice = candles.Max(c => c.High);
            double range = Math.Max(maxPrice - minPrice, 1);

            double padding = range * 0.05;
            minPrice -= padding;
            maxPrice += padding;
            range = maxPrice - minPrice;

            double candleWidth = Math.Max(2, (width - 100) / candles.Count);
            double scaleY = (upperHeight - 40) / range;

            for (int i = 0; i < candles.Count; i++)
            {
                var candle = candles[i];
                double x = 50 + i * candleWidth + candleWidth / 2;

                // Calculate positions
                double highY = 20 + (maxPrice - candle.High) * scaleY;
                double lowY = 20 + (maxPrice - candle.Low) * scaleY;
                double openY = 20 + (maxPrice - candle.Open) * scaleY;
                double closeY = 20 + (maxPrice - candle.Close) * scaleY;

                bool bullish = candle.Close > candle.Open;
                var color = bullish ? _bullishBrush : _bearishBrush;

                // Wick
                var wick = new Line
                {
                    X1 = x,
                    X2 = x,
                    Y1 = highY,
                    Y2 = lowY,
                    Stroke = Brushes.White,
                    StrokeThickness = 1
                };
                _canvas.Children.Add(wick);

                // Body
                double bodyTop = Math.Min(openY, closeY);
                double bodyBottom = Math.Max(openY, closeY);
                double bodyHeight = Math.Max(1, bodyBottom - bodyTop);

                var body = new Rectangle
                {
                    Width = Math.Max(2, candleWidth * 0.7),
                    Height = bodyHeight,
                    Fill = color,
                    Stroke = color
                };
                Canvas.SetLeft(body, x - body.Width / 2);
                Canvas.SetTop(body, bodyTop);
                _canvas.Children.Add(body);
            }

            // Price labels
            AddText($"{maxPrice:F0}", width - 45, 15, 10, _textBrush);
            AddText($"{minPrice:F0}", width - 45, upperHeight - 10, 10, _textBrush);
        }

        private void RenderCVDHistogram(List<TimeframeCandle> candles)
        {
            if (candles.Count == 0) return;

            double width = _canvas.ActualWidth;
            double height = _canvas.ActualHeight;
            double upperHeight = height * 0.7;
            double lowerHeight = height * 0.3;

            // Calculate CVD range
            double maxCVD = Math.Max(candles.Max(c => c.CVD), 0);
            double minCVD = Math.Min(candles.Min(c => c.CVD), 0);
            double range = Math.Max(Math.Abs(maxCVD), Math.Abs(minCVD)) * 1.2;
            if (range == 0) range = 1;

            double barWidth = Math.Max(2, (width - 100) / candles.Count);
            double scaleY = (lowerHeight - 40) / range;
            double zeroY = upperHeight + (lowerHeight / 2);

            for (int i = 0; i < candles.Count; i++)
            {
                var candle = candles[i];
                double x = 50 + i * barWidth + barWidth / 2;
                double cvd = candle.CVD;

                bool positive = cvd >= 0;
                double barHeight = Math.Abs(cvd * scaleY);
                var color = positive ? _cvdPositiveBrush : _cvdNegativeBrush;

                if (barHeight > 0.5)
                {
                    var bar = new Rectangle
                    {
                        Width = Math.Max(2, barWidth * 0.7),
                        Height = barHeight,
                        Fill = color,
                        Stroke = color
                    };

                    Canvas.SetLeft(bar, x - bar.Width / 2);
                    Canvas.SetTop(bar, positive ? zeroY - barHeight : zeroY);
                    _canvas.Children.Add(bar);
                }
            }

            // CVD labels
            if (maxCVD != 0)
                AddText($"+{maxCVD / 1000:F0}K", width - 45, upperHeight + 15, 8, _textBrush);
            if (minCVD != 0)
                AddText($"{minCVD / 1000:F0}K", width - 45, height - 10, 8, _textBrush);
        }

        private void RenderTimeLabels(List<TimeframeCandle> candles)
        {
            if (candles.Count == 0) return;

            double width = _canvas.ActualWidth;
            double height = _canvas.ActualHeight;
            double barWidth = Math.Max(2, (width - 100) / candles.Count);

            // Show time labels for every 5th candle or based on density
            int labelInterval = Math.Max(1, candles.Count / 8);

            for (int i = 0; i < candles.Count; i += labelInterval)
            {
                var candle = candles[i];
                double x = 50 + i * barWidth;

                string timeText = candle.Timestamp.ToString("HH:mm");
                AddText(timeText, x, height - 25, 8, _textBrush);
            }

            // Always show last candle time
            if (candles.Count > 0)
            {
                var lastCandle = candles[candles.Count - 1];
                double lastX = 50 + (candles.Count - 1) * barWidth;
                AddText(lastCandle.Timestamp.ToString("HH:mm"), lastX, height - 25, 8, _textBrush);
            }
        }

        private void AddText(string text, double x, double y, double fontSize, Brush brush)
        {
            var textBlock = new TextBlock
            {
                Text = text,
                FontSize = fontSize,
                Foreground = brush,
                FontFamily = new FontFamily("Consolas")
            };
            Canvas.SetLeft(textBlock, x);
            Canvas.SetTop(textBlock, y);
            _canvas.Children.Add(textBlock);
        }

        public void ClearData()
        {
            _currentData = null;
            _canvas.Children.Clear();
        }
    }

    public class TimeframeCandle
    {
        public DateTime Timestamp { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public double Volume { get; set; }
        public double CVD { get; set; }
        public bool IsBullish => Close > Open;
    }
}