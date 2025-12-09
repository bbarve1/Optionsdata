using Npgsql;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using static CVD.DiversionScanner;

namespace CVD
{
    #region Scanner Models
    //public class DiversionSignal : INotifyPropertyChanged
    //{
    //    public string InstrumentName { get; set; }
    //    public double Price { get; set; }
    //    public double CumulativeVolumeDelta { get; set; }
    //    public string DiversionType { get; set; }
    //    public string Strength { get; set; }
    //    public DateTime Timestamp { get; set; }

    //    public event PropertyChangedEventHandler PropertyChanged;
    //    protected virtual void OnPropertyChanged(string propertyName)
    //    {
    //        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    //    }
    //}

    public class DiversionSignal
    {
        public string InstrumentName { get; set; }
        public double Price { get; set; }
        public double PriceChangePercent { get; set; }
        public double OIChangePercent { get; set; }
        public double CVDChange { get; set; }
        public string DiversionType { get; set; }
        public string Strength { get; set; }
        public string SignalDescription { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class BigCvdTrade : INotifyPropertyChanged
    {
        public string InstrumentName { get; set; }
        public double Price { get; set; }
        public double CumulativeVolumeDelta { get; set; }
        public long Volume { get; set; }
        public string TradeSignal { get; set; }
        public string SuggestedAction { get; set; }
        public DateTime Timestamp { get; set; }
        public double BuyingPressurePercent { get; set; }
        public double SellingPressurePercent { get; set; }
        public double CvdPriceRatio { get; set; }
        public double VolumePriceRatio { get; set; }

        public double CvdZScore { get; set; }
        public string VolumeVsAverage { get; set; }
        public string CvdVsAverage { get; set; }
        public double VolumeRatio { get; set; }
        public double AverageCvd { get; set; }
        public string CvdDivergence { get; set; }  // New property
        public double PriceChangePercent { get; set; }
        public double OiChangePercent { get; set; }
        public string OiTrend { get; set; }

        public event PropertyChangedEventHandler PropertyChanged;
        protected virtual void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }

    public class CvdSpikeSignal : INotifyPropertyChanged
    {
        public string InstrumentName { get; set; }
        public double Price { get; set; }
        public double CvdSpike { get; set; }
        public double PriceChangePercent { get; set; }
        public string OIAction { get; set; }
        public string Signal { get; set; }
        public DateTime Timestamp { get; set; }

        public event PropertyChangedEventHandler PropertyChanged;
        protected virtual void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }

    public class TrendFollowingSignal : INotifyPropertyChanged
    {
        public string InstrumentName { get; set; }
        public double Price { get; set; }
        public double PriceChangePercent { get; set; }
        public double OIChangePercent { get; set; }
        public double CVDChange { get; set; }
        public string TrendDirection { get; set; }
        public string Strength { get; set; }
        public string SignalType { get; set; }
        public DateTime Timestamp { get; set; }

        public event PropertyChangedEventHandler PropertyChanged;
        protected virtual void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }

    public class ScalpingOpportunity : INotifyPropertyChanged
    {
        public string InstrumentName { get; set; }
        public double Price { get; set; }
        public double CvdSpike { get; set; }
        public double AverageCvd { get; set; }
        public double SpikeMultiplier { get; set; }
        public double PriceChangePercent { get; set; }
        public string ExpectedMove { get; set; }
        public string Confidence { get; set; }
        public DateTime Timestamp { get; set; }

        public event PropertyChangedEventHandler PropertyChanged;
        protected virtual void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }

    public class MinuteBarData
    {
        public DateTime Time { get; set; }
        public double Open { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public long Volume { get; set; }
        public double Delta { get; set; }
        public double RollingCvd { get; set; }
        public double OI { get; set; }
        public double BuyingPressurePercent { get; set; }
        public double SellingPressurePercent { get; set; }
        public double CvdPriceRatio { get; set; }

    }

    public class ScannerConfig
    {
        public double BigCvdThreshold { get; set; } = 100000;
        public double CvdSpikeThreshold { get; set; } = 50000;
        public double PriceChangeThreshold { get; set; } = 0.1;
        public int DiversionLookbackMinutes { get; set; } = 30;
    }
    #endregion

    #region Scanner Classes
    #region old dirvergion code
    //public class DiversionScanner
    //{
    //    private readonly string _connectionString;
    //    private readonly Dictionary<string, List<MinuteBarData>> _minuteDataCache;

    //    public DiversionScanner(string connectionString)
    //    {
    //        _connectionString = connectionString;
    //        _minuteDataCache = new Dictionary<string, List<MinuteBarData>>();
    //    }

        
    //    public async Task<List<MinuteBarData>> GetMinuteDataAsync(string instrumentName, DateTime startIST, DateTime endIST)
    //    {
    //        if (_minuteDataCache.TryGetValue(instrumentName, out var cachedData))
    //        {
    //            if (cachedData.Any() && cachedData[cachedData.Count - 1].Time >= DateTime.Now.AddMinutes(-5))
    //                return cachedData;
    //        }


    //        var freshData = await LoadMinuteDataFromDb(instrumentName, startIST, endIST);
    //        _minuteDataCache[instrumentName] = freshData;
    //        return freshData;
    //    }

    //    private DiversionSignal AnalyzeDiversion(string instrument, List<MinuteBarData> minuteData)
    //    {
    //        if (minuteData.Count < 2) return null;

    //        var priceTrend = CalculatePriceTrend(minuteData);
    //        var cvdTrend = CalculateCvdTrend(minuteData);

    //        // Bullish Diversion: Price down but CVD up
    //        if (priceTrend < -0.1 && cvdTrend > 10000)
    //        {
    //            return new DiversionSignal
    //            {
    //                InstrumentName = instrument,
    //                Price = minuteData[minuteData.Count - 1].Close,
    //                CumulativeVolumeDelta = minuteData[minuteData.Count - 1].RollingCvd,
    //                DiversionType = "Bullish Diversion",
    //                Strength = CalculateDiversionStrength(Math.Abs(priceTrend), cvdTrend),
    //                Timestamp = DateTime.Now
    //            };
    //        }
    //        // Bearish Diversion: Price up but CVD down
    //        else if (priceTrend > 0.1 && cvdTrend < -10000)
    //        {
    //            return new DiversionSignal
    //            {
    //                InstrumentName = instrument,
    //                Price = minuteData[minuteData.Count - 1].Close,
    //                CumulativeVolumeDelta = minuteData[minuteData.Count - 1].RollingCvd,
    //                DiversionType = "Bearish Diversion",
    //                Strength = CalculateDiversionStrength(Math.Abs(priceTrend), Math.Abs(cvdTrend)),
    //                Timestamp = DateTime.Now
    //            };
    //        }

    //        return null;
    //    }

    //    private double CalculatePriceTrend(List<MinuteBarData> data)
    //    {
    //        if (data.Count < 2) return 0;

    //        var firstPrice = data[0].Close;
    //        var lastPrice = data[data.Count - 1].Close;

    //        return (lastPrice - firstPrice) / firstPrice * 100;
    //    }

    //    private double CalculateCvdTrend(List<MinuteBarData> data)
    //    {
    //        if (data.Count < 2) return 0;

    //        var firstCvd = data[0].RollingCvd;
    //        var lastCvd = data[data.Count - 1].RollingCvd;

    //        return lastCvd - firstCvd;
    //    }


    //    private string CalculateDiversionStrength(double priceChangePercent, double cvdChange)
    //    {
    //        if (priceChangePercent > 0.5 || Math.Abs(cvdChange) > 50000)
    //            return "Strong";
    //        else if (priceChangePercent > 0.2 || Math.Abs(cvdChange) > 20000)
    //            return "Moderate";
    //        else
    //            return "Weak";
    //    }
    //    private async Task<List<MinuteBarData>> LoadMinuteDataFromDb(
    //string instrumentName,
    //DateTime startIST,
    //DateTime endIST,
    //int aggregationMinutes = 3)
    //    {
    //        var res = new List<MinuteBarData>();

    //        // Convert IST → UTC because Timescale stores in UTC
    //        var startUtc = startIST.ToUniversalTime();
    //        var endUtc = endIST.ToUniversalTime();


    //        var sql = @"
    //WITH rounded AS (
    //    SELECT 
    //        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
    //        price,
    //        size,
    //        cvd,
    //        oi
    //    FROM raw_ticks
    //    WHERE instrument_name = @instrumentName
    //      AND ts >= @startUtc
    //      AND ts <= @endUtc
    //)
    //SELECT 
    //    (date_trunc('minute', ts_ist) 
    //     - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggMin))) AS candle_time,
    //    (array_agg(price ORDER BY ts_ist ASC))[1] AS open_price,
    //    MAX(price) AS high_price,
    //    MIN(price) AS low_price,
    //    (array_agg(price ORDER BY ts_ist DESC))[1] AS close_price,
    //    SUM(size) AS total_volume,
    //    SUM(COALESCE(cvd, 0)) AS total_delta,
    //    (array_agg(oi ORDER BY ts_ist DESC))[1] AS last_oi
    //FROM rounded
    //GROUP BY candle_time
    //ORDER BY candle_time ASC;
    //";

    //        using (var conn = new NpgsqlConnection(_connectionString))
    //        {
    //            await conn.OpenAsync();
    //            using (var cmd = new NpgsqlCommand(sql, conn))
    //            {
    //                cmd.Parameters.AddWithValue("instrumentName", instrumentName);
    //                cmd.Parameters.AddWithValue("startUtc", startUtc);
    //                cmd.Parameters.AddWithValue("endUtc", endUtc);
    //                cmd.Parameters.AddWithValue("aggMin", aggregationMinutes);

    //                using (var rdr = await cmd.ExecuteReaderAsync())
    //                {
    //                    while (await rdr.ReadAsync())
    //                    {
    //                        var m = new MinuteBarData
    //                        {
    //                            Time = rdr.GetDateTime(0),
    //                            Open = rdr.IsDBNull(1) ? 0 : rdr.GetDouble(1),
    //                            High = rdr.IsDBNull(2) ? 0 : rdr.GetDouble(2),
    //                            Low = rdr.IsDBNull(3) ? 0 : rdr.GetDouble(3),
    //                            Close = rdr.IsDBNull(4) ? 0 : rdr.GetDouble(4),
    //                            Volume = rdr.IsDBNull(5) ? 0 : rdr.GetInt64(5),
    //                            Delta = rdr.IsDBNull(6) ? 0 : rdr.GetDouble(6),
    //                            OI = rdr.IsDBNull(7) ? 0 : rdr.GetDouble(7)
    //                        };
    //                        res.Add(m);
    //                    }
    //                }
    //            }
    //        }

    //        // ✅ Compute rolling CVD
    //        double rolling = 0;
    //        foreach (var m in res)
    //        {
    //            rolling += m.Delta;
    //            m.RollingCvd = rolling;
    //        }

    //        return res;
    //    }

    //    // Helper class for swing points
    //    public class DataPoint
    //    {
    //        public int Index { get; set; }
    //        public double Value { get; set; }
    //    }
       
    //}
    #endregion

    #region new code for divergence
    public class DiversionScanner
    {
        private readonly string _connectionString;
        private readonly Dictionary<string, List<MinuteBarData>> _minuteDataCache;

        public DiversionScanner(string connectionString)
        {
            _connectionString = connectionString;
            _minuteDataCache = new Dictionary<string, List<MinuteBarData>>();
        }

        public async Task<List<DiversionSignal>> ScanDiversions(List<string> instruments, int lookbackMinutes = 30)
        {
            var signals = new List<DiversionSignal>();

            foreach (var instrument in instruments)
            {
                var minuteData = await GetMinuteDataAsync(instrument, DateTime.Now.AddMinutes(-lookbackMinutes), DateTime.Now);
                if (minuteData.Count < 10) continue;

                var signal = AnalyzeStrongDivergence(instrument, minuteData, lookbackMinutes);
                if (signal != null)
                {
                    signals.Add(signal);
                }
            }

            return signals;
        }

        public async Task<List<MinuteBarData>> GetMinuteDataAsync(string instrumentName, DateTime startIST, DateTime endIST)
        {
            if (_minuteDataCache.TryGetValue(instrumentName, out var cachedData))
            {
                if (cachedData.Any() && cachedData[cachedData.Count - 1].Time >= DateTime.Now.AddMinutes(-5))
                    return cachedData;
            }

            var freshData = await LoadMinuteDataFromDb(instrumentName, startIST, endIST);
            _minuteDataCache[instrumentName] = freshData;
            return freshData;
        }

        private DiversionSignal AnalyzeStrongDivergence(string instrument, List<MinuteBarData> minuteData, int lookbackMinutes)
        {

            var recentData = TakeLast(minuteData, lookbackMinutes);
            if (recentData.Count < lookbackMinutes) return null;

            var firstBar = recentData[0];
            var lastBar = recentData[recentData.Count - 1];

            double priceChangePercent = ((lastBar.Close - firstBar.Close) / firstBar.Close) * 100;
            double oiChangePercent = firstBar.OI > 0 ? ((lastBar.OI - firstBar.OI) / firstBar.OI) * 100 : 0;
            double cvdChange = lastBar.RollingCvd - firstBar.RollingCvd;

            // Strong Bullish Divergence: Price ↘ but CVD ↗ and OI ↗ (Accumulation)
            if (priceChangePercent < -0.15 && cvdChange > 15000 && oiChangePercent > 1.0)
            {
                return new DiversionSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    DiversionType = "Strong Bullish Divergence",
                    Strength = CalculateDivergenceStrength(Math.Abs(priceChangePercent), oiChangePercent, cvdChange),
                    SignalDescription = "Price declining but CVD rising with OI increase - Strong Accumulation",
                    Timestamp = DateTime.Now
                };
            }
            // Strong Bearish Divergence: Price ↗ but CVD ↘ and OI ↗ (Distribution)
            else if (priceChangePercent > 0.15 && cvdChange < -15000 && oiChangePercent > 1.0)
            {
                return new DiversionSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    DiversionType = "Strong Bearish Divergence",
                    Strength = CalculateDivergenceStrength(Math.Abs(priceChangePercent), oiChangePercent, Math.Abs(cvdChange)),
                    SignalDescription = "Price rising but CVD falling with OI increase - Strong Distribution",
                    Timestamp = DateTime.Now
                };
            }
            // Bullish Exhaustion Divergence: Price ↘ but CVD ↗ and OI ↘ (Short Covering)
            else if (priceChangePercent < -0.15 && cvdChange > 15000 && oiChangePercent < -1.0)
            {
                return new DiversionSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    DiversionType = "Bullish Exhaustion Divergence",
                    Strength = CalculateDivergenceStrength(Math.Abs(priceChangePercent), Math.Abs(oiChangePercent), cvdChange),
                    SignalDescription = "Price declining but CVD rising with OI decrease - Short Covering",
                    Timestamp = DateTime.Now
                };
            }
            // Bearish Exhaustion Divergence: Price ↗ but CVD ↘ and OI ↘ (Long Unwinding)
            else if (priceChangePercent > 0.15 && cvdChange < -15000 && oiChangePercent < -1.0)
            {
                return new DiversionSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    DiversionType = "Bearish Exhaustion Divergence",
                    Strength = CalculateDivergenceStrength(Math.Abs(priceChangePercent), Math.Abs(oiChangePercent), Math.Abs(cvdChange)),
                    SignalDescription = "Price rising but CVD falling with OI decrease - Long Unwinding",
                    Timestamp = DateTime.Now
                };
            }
            // Classic Bullish Divergence: Price ↘ but CVD ↗
            else if (priceChangePercent < -0.1 && cvdChange > 10000)
            {
                return new DiversionSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    DiversionType = "Classic Bullish Divergence",
                    Strength = CalculateDivergenceStrength(Math.Abs(priceChangePercent), 0, cvdChange),
                    SignalDescription = "Price declining but CVD rising - Bullish Divergence",
                    Timestamp = DateTime.Now
                };
            }
            // Classic Bearish Divergence: Price ↗ but CVD ↘
            else if (priceChangePercent > 0.1 && cvdChange < -10000)
            {
                return new DiversionSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    DiversionType = "Classic Bearish Divergence",
                    Strength = CalculateDivergenceStrength(Math.Abs(priceChangePercent), 0, Math.Abs(cvdChange)),
                    SignalDescription = "Price rising but CVD falling - Bearish Divergence",
                    Timestamp = DateTime.Now
                };
            }

            return null;
        }

        private string CalculateDivergenceStrength(double priceChangePercent, double oiChangePercent, double cvdChange)
        {
            int score = 0;

            // Price movement strength
            if (priceChangePercent > 0.5) score += 3;
            else if (priceChangePercent > 0.2) score += 2;
            else if (priceChangePercent > 0.1) score += 1;

            // OI change strength
            if (Math.Abs(oiChangePercent) > 2.0) score += 3;
            else if (Math.Abs(oiChangePercent) > 1.0) score += 2;
            else if (Math.Abs(oiChangePercent) > 0.5) score += 1;

            // CVD change strength
            if (Math.Abs(cvdChange) > 50000) score += 3;
            else if (Math.Abs(cvdChange) > 25000) score += 2;
            else if (Math.Abs(cvdChange) > 10000) score += 1;

            if (score >= 7) return "Very Strong";
            if (score >= 5) return "Strong";
            if (score >= 3) return "Moderate";
            return "Weak";
        }

        private double CalculatePriceTrend(List<MinuteBarData> data)
        {
            if (data.Count < 2) return 0;

            var firstPrice = data[0].Close;
            var lastPrice = data[data.Count - 1].Close;

            return (lastPrice - firstPrice) / firstPrice * 100;
        }

        private double CalculateCvdTrend(List<MinuteBarData> data)
        {
            if (data.Count < 2) return 0;

            var firstCvd = data[0].RollingCvd;
            var lastCvd = data[data.Count - 1].RollingCvd;

            return lastCvd - firstCvd;
        }

        private async Task<List<MinuteBarData>> LoadMinuteDataFromDb(
            string instrumentName,
            DateTime startIST,
            DateTime endIST,
            int aggregationMinutes = 3)
        {
            var res = new List<MinuteBarData>();

            // Convert IST → UTC because Timescale stores in UTC
            var startUtc = startIST.ToUniversalTime();
            var endUtc = endIST.ToUniversalTime();

            var sql = @"
WITH rounded AS (
    SELECT 
        ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
        price,
        size,
        cvd,
        oi
    FROM raw_ticks
    WHERE instrument_name = @instrumentName
      AND ts >= @startUtc
      AND ts <= @endUtc
)
SELECT 
    (date_trunc('minute', ts_ist) 
     - make_interval(mins => (EXTRACT(MINUTE FROM ts_ist)::int % @aggMin))) AS candle_time,
    (array_agg(price ORDER BY ts_ist ASC))[1] AS open_price,
    MAX(price) AS high_price,
    MIN(price) AS low_price,
    (array_agg(price ORDER BY ts_ist DESC))[1] AS close_price,
    SUM(size) AS total_volume,
    SUM(COALESCE(cvd, 0)) AS total_delta,
    (array_agg(oi ORDER BY ts_ist DESC))[1] AS last_oi
FROM rounded
GROUP BY candle_time
ORDER BY candle_time ASC;
";

            using (var conn = new NpgsqlConnection(_connectionString))
            {
                await conn.OpenAsync();
                using (var cmd = new NpgsqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("instrumentName", instrumentName);
                    cmd.Parameters.AddWithValue("startUtc", startUtc);
                    cmd.Parameters.AddWithValue("endUtc", endUtc);
                    cmd.Parameters.AddWithValue("aggMin", aggregationMinutes);

                    using (var rdr = await cmd.ExecuteReaderAsync())
                    {
                        while (await rdr.ReadAsync())
                        {
                            var m = new MinuteBarData
                            {
                                Time = rdr.GetDateTime(0),
                                Open = rdr.IsDBNull(1) ? 0 : rdr.GetDouble(1),
                                High = rdr.IsDBNull(2) ? 0 : rdr.GetDouble(2),
                                Low = rdr.IsDBNull(3) ? 0 : rdr.GetDouble(3),
                                Close = rdr.IsDBNull(4) ? 0 : rdr.GetDouble(4),
                                Volume = rdr.IsDBNull(5) ? 0 : rdr.GetInt64(5),
                                Delta = rdr.IsDBNull(6) ? 0 : rdr.GetDouble(6),
                                OI = rdr.IsDBNull(7) ? 0 : rdr.GetDouble(7)
                            };
                            res.Add(m);
                        }
                    }
                }
            }

            // ✅ Compute rolling CVD
            double rolling = 0;
            foreach (var m in res)
            {
                rolling += m.Delta;
                m.RollingCvd = rolling;
            }

            return res;
        }

        private List<T> TakeLast<T>(List<T> source, int count)
        {
            if (source == null || count <= 0)
                return new List<T>();

            if (count >= source.Count)
                return source.ToList();

            return source.Skip(source.Count - count).ToList();
        }
    }

    // Supporting class for divergence signals
    
    // MinuteBarData class (assuming it exists)
   
    public class DataPoint
    {
        public int Index { get; set; }
        public double Value { get; set; }
    }
    #endregion

    // Supporting class for divergence signals


    // MinuteBarData class (assuming it exists)

    public class BigCvdScanner
    {
        private readonly Dictionary<string, double> _dynamicThresholds;
        private double _baseThreshold;

        public BigCvdScanner(double baseThreshold = 100000)
        {
            _baseThreshold = baseThreshold;
            _dynamicThresholds = new Dictionary<string, double>();
        }
        #region diversion helpers
        private string CalculateCvdDivergence(List<MinuteBarData> data)
        {
            int lookbackPeriod = 60;
            if (data.Count < lookbackPeriod) return "Insufficient Data";

            // Get last 10 bars for analysis (adjustable)
            int analysisPeriod = Math.Min(lookbackPeriod, data.Count);
            var analysisData = data.Skip(Math.Max(0, data.Count - analysisPeriod)).ToList();

            // Find significant swing highs and lows in PRICE
            var priceHighs = FindSwingHighs(analysisData, d => d.Close);
            var priceLows = FindSwingLows(analysisData, d => d.Close);

            // Find significant swing highs and lows in CVD
            var cvdHighs = FindSwingHighs(analysisData, d => d.RollingCvd);
            var cvdLows = FindSwingLows(analysisData, d => d.RollingCvd);

            // Check for Bullish Divergence (Price makes Lower Low, CVD makes Higher Low)
            if (priceLows.Count >= 2 && cvdLows.Count >= 2)
            {
                var recentPriceLow = priceLows[priceLows.Count - 1];
                var previousPriceLow = priceLows[priceLows.Count - 2];
                var recentCvdLow = cvdLows[cvdLows.Count - 1];
                var previousCvdLow = cvdLows[cvdLows.Count - 2];

                if (recentPriceLow.Value < previousPriceLow.Value &&
                    recentCvdLow.Value > previousCvdLow.Value)
                {
                    return "Bullish Divergence";
                }
            }

            // Check for Bearish Divergence (Price makes Higher High, CVD makes Lower High)
            if (priceHighs.Count >= 2 && cvdHighs.Count >= 2)
            {
                var recentPriceHigh = priceHighs[priceHighs.Count - 1];
                var previousPriceHigh = priceHighs[priceHighs.Count - 2];
                var recentCvdHigh = cvdHighs[cvdHighs.Count - 1];
                var previousCvdHigh = cvdHighs[cvdHighs.Count - 2];

                if (recentPriceHigh.Value > previousPriceHigh.Value &&
                    recentCvdHigh.Value < previousCvdHigh.Value)
                {
                    return "Bearish Divergence";
                }
            }

            // Check for Hidden Bullish Divergence (Price makes Higher Low, CVD makes Lower Low)
            if (priceLows.Count >= 2 && cvdLows.Count >= 2)
            {
                var recentPriceLow = priceLows[priceLows.Count - 1];
                var previousPriceLow = priceLows[priceLows.Count - 2];
                var recentCvdLow = cvdLows[cvdLows.Count - 1];
                var previousCvdLow = cvdLows[cvdLows.Count - 2];

                if (recentPriceLow.Value > previousPriceLow.Value &&
                    recentCvdLow.Value < previousCvdLow.Value)
                {
                    return "Hidden Bullish";
                }
            }

            // Check for Hidden Bearish Divergence (Price makes Lower High, CVD makes Higher High)
            if (priceHighs.Count >= 2 && cvdHighs.Count >= 2)
            {
                var recentPriceHigh = priceHighs[priceHighs.Count - 1];
                var previousPriceHigh = priceHighs[priceHighs.Count - 2];
                var recentCvdHigh = cvdHighs[cvdHighs.Count - 1];
                var previousCvdHigh = cvdHighs[cvdHighs.Count - 2];

                if (recentPriceHigh.Value < previousPriceHigh.Value &&
                    recentCvdHigh.Value > previousCvdHigh.Value)
                {
                    return "Hidden Bearish";
                }
            }

            return "No Divergence";
        }
        private List<DataPoint> FindSwingHighs<T>(List<T> data, Func<T, double> valueSelector, int leftRightBars = 2)
        {
            var highs = new List<DataPoint>();

            for (int i = leftRightBars; i < data.Count - leftRightBars; i++)
            {
                double currentValue = valueSelector(data[i]);
                bool isHigh = true;

                // Check if current value is higher than left and right bars
                for (int j = 1; j <= leftRightBars; j++)
                {
                    if (currentValue <= valueSelector(data[i - j]) ||
                        currentValue <= valueSelector(data[i + j]))
                    {
                        isHigh = false;
                        break;
                    }
                }

                if (isHigh)
                {
                    highs.Add(new DataPoint { Index = i, Value = currentValue });
                }
            }

            return highs;
        }

        private List<DataPoint> FindSwingLows<T>(List<T> data, Func<T, double> valueSelector, int leftRightBars = 2)
        {
            var lows = new List<DataPoint>();

            for (int i = leftRightBars; i < data.Count - leftRightBars; i++)
            {
                double currentValue = valueSelector(data[i]);
                bool isLow = true;

                // Check if current value is lower than left and right bars
                for (int j = 1; j <= leftRightBars; j++)
                {
                    if (currentValue >= valueSelector(data[i - j]) ||
                        currentValue >= valueSelector(data[i + j]))
                    {
                        isLow = false;
                        break;
                    }
                }

                if (isLow)
                {
                    lows.Add(new DataPoint { Index = i, Value = currentValue });
                }
            }

            return lows;
        }

        #endregion
        public List<BigCvdTrade> ScanBigCvdTrades(Dictionary<string, List<MinuteBarData>> instrumentData)
        {
            var trades = new List<BigCvdTrade>();

            foreach (var kvp in instrumentData)
            {
                string instrument = kvp.Key;
                List<MinuteBarData> data = kvp.Value;

                if (data.Count == 0) continue;

                var lastBar = data[data.Count - 1];
                var firstBarOfSession = data[0]; // First bar of the trading session

              
                //var threshold = GetDynamicThreshold(instrument, data);

                // Calculate new features for last 20 candles
                int lookbackPeriod = Math.Min(20, data.Count);
                var recentData = data.Skip(Math.Max(0, data.Count - lookbackPeriod)).ToList();

                // 1. Calculate CVD Z-Score
                double avgCvd = recentData.Average(d => d.RollingCvd);
                double stdDevCvd = CalculateStdDev(recentData.Select(d => d.RollingCvd));
                double cvdZScore = stdDevCvd != 0 ? (lastBar.RollingCvd - avgCvd) / stdDevCvd : 0;

                // 2. Volume above/below average
                double avgVolume = recentData.Average(d => d.Volume);
                double volumeRatio = avgVolume != 0 ? lastBar.Volume / avgVolume : 0;
                string volumeVsAvg = volumeRatio > 1 ? "Above Average" : "Below Average";

                // 3. CVD above/below average
                string cvdVsAvg = lastBar.RollingCvd > avgCvd ? "Above Average" : "Below Average";

                // 4. Calculate CVD Divergence
                string cvdDivergence = CalculateCvdDivergence(data);


                double priceChangePercent = ((lastBar.Close - firstBarOfSession.Close) / firstBarOfSession.Close) * 100;
                double oiChangePercent = firstBarOfSession.OI > 0 ? ((lastBar.OI - firstBarOfSession.OI) / firstBarOfSession.OI) * 100 : 0;
                string oiTrend = DetermineOiTrend(oiChangePercent, priceChangePercent, lastBar.OI, firstBarOfSession.OI);

                //if (Math.Abs(lastBar.RollingCvd) > threshold)
                //{
                    double totalBuyVol = data.Where(d => d.Delta > 0).Sum(d => d.Delta);
                    double totalSellVol = data.Where(d => d.Delta < 0).Sum(d => Math.Abs(d.Delta));
                    double totalVol = totalBuyVol + totalSellVol;

                    double buyPct = totalVol > 0 ? (totalBuyVol / totalVol) * 100 : 0;
                    double sellPct = totalVol > 0 ? (totalSellVol / totalVol) * 100 : 0;
                    double cvdPriceRatio = lastBar.RollingCvd / lastBar.Close;
                    double VolumePriceRatio = lastBar.Volume / lastBar.Close;

                    trades.Add(new BigCvdTrade
                    {
                        InstrumentName = instrument,
                        Price = lastBar.Close,
                        CumulativeVolumeDelta = lastBar.RollingCvd,
                        Volume = lastBar.Volume,
                        TradeSignal = lastBar.RollingCvd > 0 ? "Strong Buying Pressure" : "Strong Selling Pressure",
                        SuggestedAction = lastBar.RollingCvd > 0 ? "BUY" : "SELL",
                        BuyingPressurePercent = buyPct,
                        SellingPressurePercent = sellPct,
                        CvdPriceRatio = cvdPriceRatio,
                        VolumePriceRatio = VolumePriceRatio,
                        CvdZScore = cvdZScore,
                        VolumeVsAverage = volumeVsAvg,
                        CvdVsAverage = cvdVsAvg,
                        VolumeRatio = volumeRatio,
                        AverageCvd = avgCvd,
                        CvdDivergence = cvdDivergence,  // New column
                        PriceChangePercent = priceChangePercent,
                        OiChangePercent = oiChangePercent,
                        OiTrend = oiTrend,
                        Timestamp = DateTime.Now
                    });
                //}



            }

            return trades;
        }
        private double CalculateStdDev(IEnumerable<double> values)
        {
            double avg = values.Average();
            double sumOfSquares = values.Sum(v => (v - avg) * (v - avg));
            return Math.Sqrt(sumOfSquares / values.Count());
        }

        private string DetermineOiTrend(double oiChangePercent, double priceChangePercent, double currentOI, double previousOI)
        {
            double significantOiChange = 2.0;
            double significantPriceChange = 0.5;

            bool isOiSignificant = Math.Abs(oiChangePercent) >= significantOiChange;
            bool isPriceSignificant = Math.Abs(priceChangePercent) >= significantPriceChange;

            if (!isOiSignificant) return "Neutral";

            // Strong trend conditions
            bool strongLongBuildup = oiChangePercent > 3.0 && priceChangePercent > 1.0;
            bool strongShortBuildup = oiChangePercent > 3.0 && priceChangePercent < -1.0;
            bool strongShortCovering = oiChangePercent < -3.0 && priceChangePercent > 1.5;
            bool strongLongUnwinding = oiChangePercent < -3.0 && priceChangePercent < -1.5;

            if (strongLongBuildup) return "STRONG Long Buildup";
            if (strongShortBuildup) return "STRONG Short Buildup";
            if (strongShortCovering) return "STRONG Short Covering";
            if (strongLongUnwinding) return "STRONG Long Unwinding";

            // Regular trend conditions
            if (oiChangePercent > 1.0 && priceChangePercent > 0.5) return "Long Buildup";
            if (oiChangePercent > 1.0 && priceChangePercent < -0.5) return "Short Buildup";
            if (oiChangePercent < -1.0 && priceChangePercent > 0.8) return "Short Covering";
            if (oiChangePercent < -1.0 && priceChangePercent < -0.8) return "Long Unwinding";

            return "Neutral";
        }
        
        private double GetDynamicThreshold(string instrument, List<MinuteBarData> data)
        {
            if (_dynamicThresholds.TryGetValue(instrument, out var cachedThreshold))
                return cachedThreshold;

            if (data.Count < 5) return _baseThreshold;

            var recentData = TakeLast(data, Math.Min(10, data.Count));
            var avgVolume = recentData.Average(d => d.Volume);
            var priceVolatility = CalculatePriceVolatility(recentData);

            var dynamicThreshold = _baseThreshold * (avgVolume / 100000) * (1 + priceVolatility);
            dynamicThreshold = Math.Max(_baseThreshold, Math.Min(dynamicThreshold, _baseThreshold * 3));

            _dynamicThresholds[instrument] = dynamicThreshold;
            return dynamicThreshold;
        }

        private double CalculatePriceVolatility(List<MinuteBarData> data)
        {
            if (data.Count < 2) return 0;

            var returns = new List<double>();
            for (int i = 1; i < data.Count; i++)
            {
                var returnPct = (data[i].Close - data[i - 1].Close) / data[i - 1].Close;
                returns.Add(Math.Abs(returnPct));
            }

            return returns.Any() ? returns.Average() * 100 : 0;
        }

        // Helper method for TakeLast compatibility
        private List<T> TakeLast<T>(List<T> source, int count)
        {
            if (source == null || count <= 0)
                return new List<T>();

            if (count >= source.Count)
                return source.ToList();

            return source.Skip(source.Count - count).ToList();
        }

        public void UpdateBaseThreshold(double newThreshold)
        {
            _baseThreshold = newThreshold;
            _dynamicThresholds.Clear();
        }
    }

    public class TrendFollowingScanner
    {
        public List<TrendFollowingSignal> ScanTrendFollowing(Dictionary<string, List<MinuteBarData>> instrumentData, int lookbackMinutes = 10)
        {
            var signals = new List<TrendFollowingSignal>();

            foreach (var kvp in instrumentData)
            {
                string instrument = kvp.Key;
                List<MinuteBarData> data = kvp.Value;

                if (data.Count < lookbackMinutes) continue;

                var signal = AnalyzeTrendFollowing(instrument, data, lookbackMinutes);
                if (signal != null)
                {
                    signals.Add(signal);
                }
            }

            return signals;
        }

        private TrendFollowingSignal AnalyzeTrendFollowing(string instrument, List<MinuteBarData> data, int lookbackMinutes)
        {
            var recentData = TakeLast(data, lookbackMinutes);
            if (recentData.Count < lookbackMinutes) return null;

            var firstBar = recentData[0];
            var lastBar = recentData[recentData.Count - 1];

            double priceChangePercent = ((lastBar.Close - firstBar.Close) / firstBar.Close) * 100;
            double oiChangePercent = firstBar.OI > 0 ? ((lastBar.OI - firstBar.OI) / firstBar.OI) * 100 : 0;
            double cvdChange = lastBar.RollingCvd - firstBar.RollingCvd;

            // Bullish trend: Price ↗, OI ↗, CVD ↗
            if (priceChangePercent > 0.1 && oiChangePercent > 0.5 && cvdChange > 10000)
            {
                return new TrendFollowingSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    TrendDirection = "Bullish",
                    Strength = CalculateTrendStrength(priceChangePercent, oiChangePercent, cvdChange),
                    SignalType = "Long BuildUp with Buying Pressure",
                    Timestamp = DateTime.Now
                };
            }
            // Bearish trend: Price ↘, OI ↗, CVD ↘ (Short BuildUp)
            else if (priceChangePercent < -0.1 && oiChangePercent > 0.5 && cvdChange < -10000)
            {
                return new TrendFollowingSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    TrendDirection = "Bearish",
                    Strength = CalculateTrendStrength(Math.Abs(priceChangePercent), oiChangePercent, Math.Abs(cvdChange)),
                    SignalType = "Short BuildUp with Selling Pressure",
                    Timestamp = DateTime.Now
                };
            }
            // Bullish Covering: Price ↗, OI ↘, CVD ↗
            else if (priceChangePercent > 0.1 && oiChangePercent < -0.5 && cvdChange > 10000)
            {
                return new TrendFollowingSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    TrendDirection = "Bullish",
                    Strength = CalculateTrendStrength(priceChangePercent, Math.Abs(oiChangePercent), cvdChange),
                    SignalType = "Short Covering with Buying",
                    Timestamp = DateTime.Now
                };
            }
            // Bearish Unwinding: Price ↘, OI ↘, CVD ↘
            else if (priceChangePercent < -0.1 && oiChangePercent < -0.5 && cvdChange < -10000)
            {
                return new TrendFollowingSignal
                {
                    InstrumentName = instrument,
                    Price = lastBar.Close,
                    PriceChangePercent = priceChangePercent,
                    OIChangePercent = oiChangePercent,
                    CVDChange = cvdChange,
                    TrendDirection = "Bearish",
                    Strength = CalculateTrendStrength(Math.Abs(priceChangePercent), Math.Abs(oiChangePercent), Math.Abs(cvdChange)),
                    SignalType = "Long Unwinding with Selling",
                    Timestamp = DateTime.Now
                };
            }

            return null;
        }

        private string CalculateTrendStrength(double priceChange, double oiChange, double cvdChange)
        {
            var score = (priceChange * 0.4) + (oiChange * 0.3) + (cvdChange / 10000 * 0.3);

            if (score > 2.0) return "Very Strong";
            if (score > 1.0) return "Strong";
            if (score > 0.5) return "Moderate";
            return "Weak";
        }

        // Helper method for TakeLast compatibility
        private List<T> TakeLast<T>(List<T> source, int count)
        {
            if (source == null || count <= 0)
                return new List<T>();

            if (count >= source.Count)
                return source.ToList();

            return source.Skip(source.Count - count).ToList();
        }
    }

    public class ScalpingScanner
    {
        private readonly double _minSpikeMultiplier;
        private readonly double _maxPriceChangePercent;

        public ScalpingScanner(double minSpikeMultiplier = 5.0, double maxPriceChangePercent = 0.05)
        {
            _minSpikeMultiplier = minSpikeMultiplier;
            _maxPriceChangePercent = maxPriceChangePercent;
        }

        public List<ScalpingOpportunity> ScanScalpingOpportunities(Dictionary<string, List<MinuteBarData>> instrumentData, int lookbackForAverage = 20)
        {
            var opportunities = new List<ScalpingOpportunity>();

            foreach (var kvp in instrumentData)
            {
                string instrument = kvp.Key;
                List<MinuteBarData> data = kvp.Value;

                if (data.Count < lookbackForAverage + 1) continue;

                var opportunity = AnalyzeScalpingOpportunity(instrument, data, lookbackForAverage);
                if (opportunity != null)
                {
                    opportunities.Add(opportunity);
                }
            }

            return opportunities;
        }

        private ScalpingOpportunity AnalyzeScalpingOpportunity(string instrument, List<MinuteBarData> data, int lookbackForAverage)
        {
            if (data.Count < 2) return null;

            var currentBar = data[data.Count - 1];
            var previousBar = data[data.Count - 2];

            // Calculate average CVD for lookback period (excluding current minute)
            var lookbackData = TakeLast(data.Take(data.Count - 1).ToList(), lookbackForAverage);
            if (lookbackData.Count < lookbackForAverage / 2) return null;

            double averageCvd = lookbackData.Average(d => Math.Abs(d.Delta));
            double currentCvd = Math.Abs(currentBar.Delta);
            double spikeMultiplier = averageCvd > 0 ? currentCvd / averageCvd : 0;

            double priceChangePercent = Math.Abs((currentBar.Close - previousBar.Close) / previousBar.Close * 100);

            // Criteria: CVD spike > 5x average AND price not changed much
            if (spikeMultiplier >= _minSpikeMultiplier && priceChangePercent <= _maxPriceChangePercent)
            {
                return new ScalpingOpportunity
                {
                    InstrumentName = instrument,
                    Price = currentBar.Close,
                    CvdSpike = currentBar.Delta,
                    AverageCvd = averageCvd,
                    SpikeMultiplier = spikeMultiplier,
                    PriceChangePercent = priceChangePercent,
                    ExpectedMove = currentBar.Delta > 0 ? "Bullish Breakout" : "Bearish Breakdown",
                    Confidence = CalculateConfidenceLevel(spikeMultiplier, priceChangePercent),
                    Timestamp = DateTime.Now
                };
            }

            return null;
        }

        private string CalculateConfidenceLevel(double spikeMultiplier, double priceChangePercent)
        {
            var confidenceScore = (spikeMultiplier / _minSpikeMultiplier) * (1 - (priceChangePercent / _maxPriceChangePercent));

            if (confidenceScore > 2.0) return "Very High";
            if (confidenceScore > 1.5) return "High";
            if (confidenceScore > 1.0) return "Medium";
            return "Low";
        }

        // Helper method for TakeLast compatibility
        private List<T> TakeLast<T>(List<T> source, int count)
        {
            if (source == null || count <= 0)
                return new List<T>();

            if (count >= source.Count)
                return source.ToList();

            return source.Skip(source.Count - count).ToList();
        }
    }

    public class EnhancedCvdSpikeScanner
    {
        private readonly double _priceChangeThreshold;
        private readonly double _minSpikeMultiplier;

        public EnhancedCvdSpikeScanner(double priceChangeThreshold = 0.05, double minSpikeMultiplier = 5.0)
        {
            _priceChangeThreshold = priceChangeThreshold;
            _minSpikeMultiplier = minSpikeMultiplier;
        }

        public List<ScalpingOpportunity> ScanEnhancedCvdSpikes(Dictionary<string, List<MinuteBarData>> instrumentData, int lookbackForAverage = 15)
        {
            var scalpingScanner = new ScalpingScanner(_minSpikeMultiplier, _priceChangeThreshold);
            return scalpingScanner.ScanScalpingOpportunities(instrumentData, lookbackForAverage);
        }

        public List<CvdSpikeSignal> ScanCvdSpikes(Dictionary<string, List<MinuteBarData>> instrumentData)
        {
            var signals = new List<CvdSpikeSignal>();

            foreach (var kvp in instrumentData)
            {
                string instrument = kvp.Key;
                List<MinuteBarData> data = kvp.Value;

                if (data.Count < 2) continue;

                var signal = AnalyzeCvdSpike(instrument, data);
                if (signal != null)
                {
                    signals.Add(signal);
                }
            }

            return signals;
        }

        private CvdSpikeSignal AnalyzeCvdSpike(string instrument, List<MinuteBarData> data)
        {
            if (data.Count < 2) return null;

            var lastTwoMinutes = TakeLast(data, 2);
            if (lastTwoMinutes.Count < 2) return null;

            var currentBar = lastTwoMinutes[1];
            var previousBar = lastTwoMinutes[0];

            // Calculate average CVD for context
            var lookbackData = TakeLast(data.Take(data.Count - 1).ToList(), 10);
            double averageCvd = lookbackData.Any() ? lookbackData.Average(d => Math.Abs(d.Delta)) : 0;
            double spikeMultiplier = averageCvd > 0 ? Math.Abs(currentBar.Delta) / averageCvd : 0;

            double priceChangePercent = Math.Abs((currentBar.Close - previousBar.Close) / previousBar.Close * 100);

            if (spikeMultiplier >= _minSpikeMultiplier && priceChangePercent < _priceChangeThreshold)
            {
                var oiAction = DetermineOIAction(currentBar, previousBar);
                var signal = GenerateSignal(currentBar.Delta, oiAction, spikeMultiplier);

                return new CvdSpikeSignal
                {
                    InstrumentName = instrument,
                    Price = currentBar.Close,
                    CvdSpike = currentBar.Delta,
                    PriceChangePercent = priceChangePercent,
                    OIAction = oiAction,
                    Signal = signal,
                    Timestamp = DateTime.Now
                };
            }

            return null;
        }

        private string DetermineOIAction(MinuteBarData current, MinuteBarData previous)
        {
            bool priceUp = current.Close > previous.Close;
            bool oiUp = current.OI > previous.OI;

            if (priceUp && oiUp) return "Long BuildUp";
            if (priceUp && !oiUp) return "Short Covering";
            if (!priceUp && oiUp) return "Short BuildUp";
            if (!priceUp && !oiUp) return "Long Unwind";

            return "Neutral";
        }

        private string GenerateSignal(double delta, string oiAction, double spikeMultiplier)
        {
            string strength = spikeMultiplier > 2 ? "Very Strong" : spikeMultiplier > 7 ? "Strong" : "Moderate";

            if (delta > 0)
            {
                if (oiAction == "Long BuildUp") return $"{strength} Bullish (BuildUp)";
                if (oiAction == "Short Covering") return $"{strength} Bullish (Covering)";
                return $"{strength} Bullish";
            }
            else
            {
                if (oiAction == "Short BuildUp") return $"{strength} Bearish (BuildUp)";
                if (oiAction == "Long Unwind") return $"{strength} Bearish (Unwinding)";
                return $"{strength} Bearish";
            }
        }

        // Helper method for TakeLast compatibility
        private List<T> TakeLast<T>(List<T> source, int count)
        {
            if (source == null || count <= 0)
                return new List<T>();

            if (count >= source.Count)
                return source.ToList();

            return source.Skip(source.Count - count).ToList();
        }
    }
    #endregion

    #region Main Scanner Service
    public class EnhancedScannerService
    {
        private readonly DiversionScanner _diversionScanner;
        private readonly BigCvdScanner _bigCvdScanner;
        private readonly EnhancedCvdSpikeScanner _cvdSpikeScanner;
        private readonly TrendFollowingScanner _trendFollowingScanner;
        private readonly ScalpingScanner _scalpingScanner;

        public EnhancedScannerService(string connectionString)
        {
            _diversionScanner = new DiversionScanner(connectionString);
            _bigCvdScanner = new BigCvdScanner(100000);
            _cvdSpikeScanner = new EnhancedCvdSpikeScanner(0.05, 5.0);
            _trendFollowingScanner = new TrendFollowingScanner();
            _scalpingScanner = new ScalpingScanner(5.0, 0.05);
        }

        public async Task<EnhancedScannerResults> RunAllScanners(List<string> instruments, DateTime startIST,DateTime endIST)
        {
            var results = new EnhancedScannerResults();

            try
            {
                //var startIST = new DateTime(2025, 11, 12, 9, 15, 0);   
                //var endIST = new DateTime(2025, 11, 12, 9, 17, 0); 

                //var data = await LoadMinuteDataFromDb("RELIANCE", startIST, endIST);
                // Get minute data for all instruments
                

                var instrumentData = new Dictionary<string, List<MinuteBarData>>();
                foreach (var instrument in instruments)
                {
                    var data = await _diversionScanner.GetMinuteDataAsync(instrument, startIST, endIST);
                    instrumentData[instrument] = data;
                }

                // Run all scanners
                //var diversion30MinTask = _diversionScanner.ScanDiversions(instruments, 30);
                //var diversion1HourTask = _diversionScanner.ScanDiversions(instruments, 60);
                var bigCvdTask = Task.Run(() => _bigCvdScanner.ScanBigCvdTrades(instrumentData));
                var cvdSpikeTask = Task.Run(() => _cvdSpikeScanner.ScanCvdSpikes(instrumentData));
                var trendFollowingTask = Task.Run(() => _trendFollowingScanner.ScanTrendFollowing(instrumentData, 10));
                var scalpingTask = Task.Run(() => _scalpingScanner.ScanScalpingOpportunities(instrumentData, 20));

                // Wait for all tasks to complete
                //await Task.WhenAll(diversion30MinTask, diversion1HourTask, bigCvdTask, cvdSpikeTask, trendFollowingTask, scalpingTask);
                await Task.WhenAll(bigCvdTask, cvdSpikeTask, trendFollowingTask, scalpingTask);

                //results.Diversion30Min = diversion30MinTask.Result;
                //results.Diversion1Hour = diversion1HourTask.Result;
                results.BigCvdTrades = bigCvdTask.Result;
                results.CvdSpikes = cvdSpikeTask.Result;
                results.TrendFollowingSignals = trendFollowingTask.Result;
                results.ScalpingOpportunities = scalpingTask.Result;
                results.Success = true;
            }
            catch (Exception ex)
            {
                results.ErrorMessage = ex.Message;
                results.Success = false;
            }

            return results;
        }
    }

    public class EnhancedScannerResults
    {
        public bool Success { get; set; }
        public string ErrorMessage { get; set; }
        public List<DiversionSignal> Diversion30Min { get; set; } = new List<DiversionSignal>();
        public List<DiversionSignal> Diversion1Hour { get; set; } = new List<DiversionSignal>();
        public List<BigCvdTrade> BigCvdTrades { get; set; } = new List<BigCvdTrade>();
        public List<CvdSpikeSignal> CvdSpikes { get; set; } = new List<CvdSpikeSignal>();
        public List<TrendFollowingSignal> TrendFollowingSignals { get; set; } = new List<TrendFollowingSignal>();
        public List<ScalpingOpportunity> ScalpingOpportunities { get; set; } = new List<ScalpingOpportunity>();
    }
    #endregion
}