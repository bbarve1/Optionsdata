using LiveCharts;
using LiveCharts.Defaults;
using LiveCharts.Wpf;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;

namespace CVD
{
    public class DrawCharts
    {
        private const string ConnStr = "Host=localhost;Port=5432;Username=postgres;Password=Tripleb@003;Database=marketdata";

        private ComboBox SymbolCombo;
        private ComboBox TimeframeCombo;
        private DatePicker DatePickerBox;
        private CartesianChart PriceChart;
        private CartesianChart CvdChart;

        public DrawCharts(ComboBox symbolCombo, ComboBox timeframeCombo, DatePicker datePicker, CartesianChart priceChart, CartesianChart cvdChart)
        {
            SymbolCombo = symbolCombo;
            TimeframeCombo = timeframeCombo;
            DatePickerBox = datePicker;
            PriceChart = priceChart;
            CvdChart = cvdChart;
        }

        public void LoadSymbols()
        {
            using (var con = new NpgsqlConnection(ConnStr))
            {
                con.Open();
                var cmd = new NpgsqlCommand("SELECT DISTINCT instrument_name FROM raw_ticks ORDER BY instrument_name;", con);
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                    SymbolCombo.Items.Add(reader.GetString(0));
            }

            if (SymbolCombo.Items.Count > 0)
                SymbolCombo.SelectedIndex = 0;

            TimeframeCombo.SelectedIndex = 0;
            DatePickerBox.SelectedDate = DateTime.Now.Date;
        }

        public async Task LoadChartAsync()
        {
            if (SymbolCombo.SelectedItem == null || TimeframeCombo.SelectedItem == null || DatePickerBox.SelectedDate == null)
                return;

            string symbol = SymbolCombo.SelectedItem.ToString();
            int timeframe = int.Parse(((ComboBoxItem)TimeframeCombo.SelectedItem).Content.ToString());
            DateTime selectedDate = DatePickerBox.SelectedDate.Value;

            var ticks = await LoadTicks(symbol, selectedDate);
            if (!ticks.Any()) return;

            var bars = AggregateTicksToBars(ticks, timeframe);

            DrawPriceChart(bars);
            DrawCvdChart(bars);
        }

        private async Task<List<Tick>> LoadTicks(string symbol, DateTime date)
        {
            var ticks = new List<Tick>();

            using (var con = new NpgsqlConnection(ConnStr))
            {
                await con.OpenAsync();
                string query = @"SELECT ts, price, size, bid_price, ask_price 
                                 FROM raw_ticks 
                                 WHERE instrument_name = @symbol 
                                   AND ts::date = @date
                                 ORDER BY ts;";

                using (var cmd = new NpgsqlCommand(query, con))
                {
                    cmd.Parameters.AddWithValue("@symbol", symbol);
                    cmd.Parameters.AddWithValue("@date", date);

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            ticks.Add(new Tick
                            {
                                Timestamp = reader.GetDateTime(0).ToLocalTime(),
                                Price = reader.GetDouble(1),
                                Volume = reader.GetInt64(2),
                                Bid = reader.GetDouble(3),
                                Ask = reader.GetDouble(4)
                            });
                        }
                    }
                }
            }

            return ticks;
        }

        private List<Candle> AggregateTicksToBars(List<Tick> ticks, int timeframeMin)
        {
            var bars = new List<Candle>();
            if (ticks.Count == 0) return bars;

            var grouped = ticks.GroupBy(t => new DateTime(
                t.Timestamp.Year, t.Timestamp.Month, t.Timestamp.Day,
                t.Timestamp.Hour, (t.Timestamp.Minute / timeframeMin) * timeframeMin, 0));

            double cumulativeCvd = 0;

            foreach (var g in grouped)
            {
                var list = g.OrderBy(x => x.Timestamp).ToList();
                double open = list.First().Price;
                double close = list.Last().Price;
                double high = list.Max(x => x.Price);
                double low = list.Min(x => x.Price);
                long vol = list.Sum(x => x.Volume);

                // Tick-by-tick true CVD logic
                double upVol = 0, downVol = 0;
                double prevPrice = list.First().Price;

                foreach (var t in list)
                {
                    if (t.Price > prevPrice)
                        upVol += t.Volume;
                    else if (t.Price < prevPrice)
                        downVol += t.Volume;

                    prevPrice = t.Price;
                }

                cumulativeCvd += (upVol - downVol);

                bars.Add(new Candle
                {
                    Time = g.Key,
                    Open = open,
                    High = high,
                    Low = low,
                    Close = close,
                    Volume = vol,
                    Cvd = cumulativeCvd
                });
            }

            return bars.OrderBy(x => x.Time).ToList();
        }

        private void DrawPriceChart(List<Candle> bars)
        {
            if (bars == null || !bars.Any()) return;

            var candleSeries = new CandleSeries
            {
                Title = "Price",
                Values = new ChartValues<OhlcPoint>(bars.Select(b => new OhlcPoint(b.Open, b.High, b.Low, b.Close))),
                IncreaseBrush = Brushes.LimeGreen,
                DecreaseBrush = Brushes.Red,
                MaxColumnWidth = 25,
                StrokeThickness = 1,
                Stroke = Brushes.DarkGray
            };

            PriceChart.Series = new SeriesCollection { candleSeries };
            PriceChart.AxisX.Clear();
            PriceChart.AxisY.Clear();

            // X-axis with proper step calculation
            PriceChart.AxisX.Add(new Axis
            {
                Labels = bars.Select(b => b.Time.ToString("HH:mm")).ToArray(),
                Foreground = Brushes.White,
                Separator = new LiveCharts.Wpf.Separator
                {
                    Step = CalculateStep(bars.Count),
                    StrokeThickness = 0.5,
                    Stroke = new SolidColorBrush(Color.FromArgb(30, 255, 255, 255))
                }
            });

            // Y-axis with proper scaling
            double minPrice = bars.Min(b => b.Low);
            double maxPrice = bars.Max(b => b.High);
            double range = maxPrice - minPrice;

            PriceChart.AxisY.Add(new Axis
            {
                Title = "Price",
                Foreground = Brushes.White,
                LabelFormatter = value => value.ToString("N2"),
                MinValue = minPrice - (range * 0.01), // 1% padding
                MaxValue = maxPrice + (range * 0.01),
                Separator = new LiveCharts.Wpf.Separator
                {
                    StrokeThickness = 0.5,
                    Stroke = new SolidColorBrush(Color.FromArgb(30, 255, 255, 255))
                }
            });

            PriceChart.Background = new SolidColorBrush(Color.FromRgb(30, 30, 30));
            PriceChart.LegendLocation = LegendLocation.None;
            PriceChart.Zoom = ZoomingOptions.Xy;

            // Set minimum width for scrollable content
            PriceChart.MinWidth = bars.Count * 30; // 30 pixels per candle
        }

        private void DrawCvdChart(List<Candle> bars)
        {
            if (bars == null || bars.Count == 0)
                return;

            // ---- Build OHLC-like CVD bars ----
            var cvdCandleValues = new ChartValues<OhlcPoint>();

            double prevCvd = bars[0].Cvd;

            foreach (var b in bars)
            {
                double open = prevCvd;
                double close = b.Cvd;
                double high = Math.Max(open, close);
                double low = Math.Min(open, close);

                cvdCandleValues.Add(new OhlcPoint(open, high, low, close));

                prevCvd = close;
            }

            // ---- X-axis time labels ----
            var timeLabels = bars.Select(b => b.Time.ToString("HH:mm")).ToArray();

            // ---- Create CandleSeries for CVD ----
            var cvdSeries = new CandleSeries
            {
                Title = "CVD",
                Values = cvdCandleValues,
                IncreaseBrush = Brushes.LimeGreen,
                DecreaseBrush = Brushes.Red,
                MaxColumnWidth = 25,
                StrokeThickness = 1,
                Stroke = Brushes.DarkGray
            };

            // ---- Apply to chart ----
            CvdChart.Series = new SeriesCollection { cvdSeries };

            CvdChart.AxisX.Clear();
            CvdChart.AxisX.Add(new Axis
            {
                Labels = timeLabels,
                LabelsRotation = 0,
                Foreground = Brushes.White,
                Separator = new LiveCharts.Wpf.Separator()
                {
                    Step = CalculateStep(bars.Count),
                    StrokeThickness = 0.5,
                    Stroke = new SolidColorBrush(Color.FromArgb(30, 255, 255, 255))
                }
            });

            CvdChart.AxisY.Clear();
            CvdChart.AxisY.Add(new Axis
            {
                Title = "Cumulative Volume Δ",
                Foreground = Brushes.White,
                LabelFormatter = v => v.ToString("N0"),
                Separator = new LiveCharts.Wpf.Separator()
                {
                    StrokeThickness = 0.5,
                    Stroke = new SolidColorBrush(Color.FromArgb(30, 255, 255, 255))
                }
            });

            CvdChart.Background = new SolidColorBrush(Color.FromRgb(30, 30, 30));
            CvdChart.LegendLocation = LegendLocation.None;

            // Set minimum width for scrollable content
            CvdChart.MinWidth = bars.Count * 30; // 30 pixels per candle
        }

        private double CalculateStep(int barCount)
        {
            if (barCount <= 15) return 1;
            if (barCount <= 30) return 2;
            if (barCount <= 60) return 3;
            if (barCount <= 120) return 5;
            return Math.Max(1, barCount / 20);
        }

        public class Tick
        {
            public DateTime Timestamp { get; set; }
            public double Price { get; set; }
            public long Volume { get; set; }
            public double Bid { get; set; }
            public double Ask { get; set; }
        }

        public class Candle
        {
            public DateTime Time { get; set; }
            public double Open { get; set; }
            public double High { get; set; }
            public double Low { get; set; }
            public double Close { get; set; }
            public long Volume { get; set; }
            public double Cvd { get; set; }
        }
    }
}