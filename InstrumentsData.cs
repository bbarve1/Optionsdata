using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Xml.Linq;
using static MongoDB.Bson.Serialization.Serializers.SerializerHelper;

namespace CVD
{
    public class InstrumentsData
    {
        private const string BodJsonUrl = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz";
        private static string baseUrl = "https://api.upstox.com";
        private readonly string _accessToken;
        private readonly Dictionary<string, string> _instrumentNameMap = new Dictionary<string, string>();
        public InstrumentsData(string accessToken)
        {
            _accessToken = accessToken;

        }



        public class Instrument
        {
            public string instrument_key { get; set; }
            public string segment { get; set; }
            public string instrument_type { get; set; }
            public string name { get; set; }
            public string trading_symbol { get; set; }
            public string expiry { get; set; }
            public double strike_price { get; set; }
        }

        public async Task<(List<Instrument> equityList, List<Instrument> fnoList, List<Instrument> niftyCE, List<Instrument> niftyPE, List<Instrument> bankniftyCE, List<Instrument> bankniftyPE, Dictionary<string, string> InstrumentNameMap)> GetInstrumentsAsync()
        {

            var client = new RestClient();
            var request = new RestRequest(BodJsonUrl, Method.Get);
            var response = client.Execute(request);

            if (!response.IsSuccessful)
                throw new Exception($"Error fetching instruments: {response.StatusCode} - {response.Content}");


            // Step 2: Decompress JSON
            List<Instrument> instruments;
            using (var compressedStream = new MemoryStream(response.RawBytes))
            using (var gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
            using (var reader = new StreamReader(gzipStream))
            {
                var json = reader.ReadToEnd();
                instruments = JsonConvert.DeserializeObject<List<Instrument>>(json);
            }

            string instrumentKey = "NSE_INDEX%7CNifty%2050";
            var nifty50Symbols = new HashSet<string>
{
                "RELIANCE", "TCS", "HDFCBANK", "INFY", "HINDUNILVR", "ICICIBANK", "KOTAKBANK",
                "SBIN", "BHARTIARTL", "ITC", "ASIANPAINT", "DMART", "BAJFINANCE", "HCLTECH",
                "WIPRO", "SUNPHARMA", "MARUTI", "TITAN", "ULTRACEMCO", "NTPC", "ONGC",
                "POWERGRID", "NESTLEIND", "M&M", "AXISBANK", "LT", "TECHM", "TATAMOTORS",
                "ADANIPORTS", "BAJAJFINSV", "BRITANNIA", "GRASIM", "JSWSTEEL", "HDFCLIFE",
                "CIPLA", "SHREECEM", "UPL", "SBILIFE", "DIVISLAB", "DRREDDY", "HEROMOTOCO",
                "INDUSINDBK", "COALINDIA", "BAJAJ-AUTO", "EICHERMOT", "APOLLOHOSP", "TATASTEEL", "BPCL"
            };

            //var nifty50Symbols = new HashSet<string>
            //        {
            //            "HAL", "NAUKRI", "HAL","BHARTIARTL"
            //        };
            var equityList = instruments
                            .Where(x => nifty50Symbols.Contains(x.trading_symbol))
                            .ToList();

            double niftySpot = 25300;
            double bankNiftySpot = 55000;

            var equityNames = equityList
                .Select(x => x.name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var fnoSymbols = instruments
                .Where(x => x.segment == "NSE_FO"
                            && x.instrument_type == "FUT"
                            && equityNames.Contains(x.name))
                .Select(x => x.name)
                .Distinct()
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            //var equityList = instruments
            //    .Where(x => x.segment == "NSE_EQ" && x.instrument_type == "EQ" && fnoSymbols.Contains(x.name))
            //    .ToList();

            // 4️⃣ FNO list — only for equities in our equityList
            var fnoList = instruments
                            .Where(x => x.segment == "NSE_FO"
                                        && fnoSymbols.Contains(x.name)
                                        && !x.trading_symbol.EndsWith("CE") // Exclude Call Options
                                        && !x.trading_symbol.EndsWith("PE") // Exclude Put Options
                                        && x.instrument_type == "FUT") // Keep only Futures
                            .ToList();

            var expiry = fnoList
                            .Select(x => ParseExpiry(x.expiry))
                            .OrderBy(d => d)
                            .First();



            var ExpiryDate = expiry.Date;

            fnoList = fnoList
                            .Where(x => ParseExpiry(x.expiry).Date == ExpiryDate.Date)
                             .ToList();

            var niftyOptionsAll = instruments
                 .Where(x => x.name != null
                             && x.name.Equals("NIFTY", StringComparison.OrdinalIgnoreCase)
                             && (x.expiry != ""))
                 .ToList();


            // 2️⃣ Find nearest expiry
            var nearestExpiry = niftyOptionsAll
                            .Select(x => ParseExpiry(x.expiry))
                            .OrderBy(d => d)
                            .First();

            // 3️⃣ Live spot price (replace with API fetched)
            //double niftySpot = 22450;

            // 4️⃣ ±8 strikes from spot (strike difference for NIFTY = 50)
            var nearestExpiryDate = nearestExpiry.Date;
            
            var niftyOptions = niftyOptionsAll
                            .Where(x => ParseExpiry(x.expiry).Date == nearestExpiry.Date
                                        && Math.Abs(x.strike_price - niftySpot) <= (1 * 50))
                            .OrderBy(x => x.strike_price)
                            .ToList();

            var niftyCE = niftyOptions.Where(x => x.instrument_type == "CE").ToList();
            var niftyPE = niftyOptions.Where(x => x.instrument_type == "PE").ToList();

            // 6️⃣ BANKNIFTY options (nearest expiry ±8 strikes)
            var bankniftySpot = 55000; // Replace with live spot from API
            var bankniftyOptionsAll = instruments
                 .Where(x => x.name != null
                             && x.name.Equals("BANKNIFTY", StringComparison.OrdinalIgnoreCase)
                             && (x.expiry != ""))
                 .ToList();


            // 2️⃣ Find nearest expiry
            var banknearestExpiry = bankniftyOptionsAll
                            .Select(x => ParseExpiry(x.expiry))
                            .OrderBy(d => d)
                            .First();


            // 4️⃣ ±8 strikes from spot (strike difference for NIFTY = 50)
            var banknearestExpiryDate = banknearestExpiry.Date;

            var bankniftyOptions = bankniftyOptionsAll
                            .Where(x => ParseExpiry(x.expiry).Date == banknearestExpiryDate.Date
                                        && Math.Abs(x.strike_price - bankniftySpot) <= (8 * 50))
                            .OrderBy(x => x.strike_price)
                            .ToList();

            var bankniftyCE = bankniftyOptions.Where(x => x.instrument_type == "CE").ToList();
            var bankniftyPE = bankniftyOptions.Where(x => x.instrument_type == "PE").ToList();
            var instrumentNameMap = new Dictionary<string, string>();
            foreach (var inst in instruments)
            {
                _instrumentNameMap[inst.instrument_key] = inst.trading_symbol; // or inst.name
            }



            await Task.CompletedTask; // To satisfy async method signature  
            return (equityList, fnoList, niftyCE, niftyPE, bankniftyCE, bankniftyPE, instrumentNameMap);

        }

        private DateTime ParseExpiry(string expiry)
        {
            // Convert milliseconds since epoch to DateTime (UTC)
            if (long.TryParse(expiry, out long ms))
            {
                DateTimeOffset dto = DateTimeOffset.FromUnixTimeMilliseconds(ms);
                return dto.UtcDateTime.Date; // Take only the date part
            }

            throw new FormatException($"Invalid expiry value: {expiry}");
        }
        

    }

   
       
    }
