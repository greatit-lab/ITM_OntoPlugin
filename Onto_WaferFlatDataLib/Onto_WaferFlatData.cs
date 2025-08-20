// Onto_WaferFlatDataLib/Onto_WaferFlatData.cs
using ITM_Agent.Common;
using ITM_Agent.Common.Interfaces;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Onto_WaferFlatDataLib
{
    /// <summary>
    /// Wafer Flat 데이터를 파싱하여 데이터베이스에 업로드하는 플러그인입니다.
    /// IPlugin 인터페이스를 구현하여 ITM_Agent와 독립적으로 동작합니다.
    /// </summary>
    public class Onto_WaferFlatData : IPlugin
    {
        private ISettingsManager _settings;
        private ILogManager _logger;
        private ITimeSyncProvider _timeSync;

        public string Name => "Onto_WaferFlatData";
        public string Version => Assembly.GetExecutingAssembly().GetName().Version.ToString();

        public void Initialize(ISettingsManager settings, ILogManager logger, ITimeSyncProvider timeSync)
        {
            _settings = settings;
            _logger = logger;
            _timeSync = timeSync;

            try
            {
                Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            }
            catch (InvalidOperationException)
            {
                _logger.LogDebug($"[{Name}] CodePagesEncodingProvider is already registered.");
            }
            _logger.LogEvent($"[{Name}] Plugin initialized. Version: {Version}");
        }

        public void SetDebugMode(bool isEnabled)
        {
            SimpleLogger.SetDebugMode(isEnabled);
        }

        public void Execute(string filePath)
        {
            _logger.LogEvent($"[{Name}] Execute called for file: {Path.GetFileName(filePath)}");

            if (!WaitForFileReady(filePath, 20, 500))
            {
                _logger.LogEvent($"[{Name}] SKIPPED (file is locked): {Path.GetFileName(filePath)}");
                return;
            }

            try
            {
                ProcessFile(filePath);
            }
            catch (Exception ex)
            {
                _logger.LogError($"[{Name}] An unhandled exception occurred while processing {Path.GetFileName(filePath)}: {ex.Message}");
                _logger.LogDebug($"[{Name}] Unhandled Exception Details: {ex.ToString()}");
                SimpleLogger.Error($"Unhandled EXCEPTION: {ex.ToString()}");
            }
        }

        private void ProcessFile(string filePath)
        {
            string eqpid = _settings.GetEqpid();
            _logger.LogDebug($"[{Name}] Reading file content for: {Path.GetFileName(filePath)} with Eqpid: {eqpid}");
            string fileContent = ReadAllTextSafe(filePath, Encoding.GetEncoding(949));
            if (string.IsNullOrEmpty(fileContent))
            {
                _logger.LogEvent($"[{Name}] File is empty, skipping processing: {Path.GetFileName(filePath)}");
                return;
            }
            var lines = fileContent.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);

            _logger.LogDebug($"[{Name}] Parsing metadata from {lines.Length} lines.");
            var meta = ParseMetadata(lines);
            var (waferNo, dateTime) = ExtractPrimaryMeta(meta);

            _logger.LogDebug($"[{Name}] Searching for data header index.");
            int headerIndex = FindHeaderIndex(lines);
            if (headerIndex == -1)
            {
                _logger.LogError($"[{Name}] Header row starting with 'Point#' not found in '{Path.GetFileName(filePath)}'. Skipping file.");
                return;
            }
            _logger.LogDebug($"[{Name}] Header found at line index {headerIndex}.");

            var rows = ParseDataRows(lines, headerIndex, meta, waferNo, dateTime);
            if (rows.Count == 0)
            {
                _logger.LogEvent($"[{Name}] No valid data rows were parsed from '{Path.GetFileName(filePath)}'. Skipping DB upload.");
                return;
            }
             _logger.LogDebug($"[{Name}] Successfully parsed {rows.Count} data rows. Building DataTable.");

            var dataTable = BuildDataTable(rows, eqpid);
            UploadToDatabase(dataTable, Path.GetFileName(filePath));

            try
            {
                _logger.LogDebug($"[{Name}] Deleting processed file: {filePath}");
                File.Delete(filePath);
                _logger.LogEvent($"[{Name}] Successfully deleted processed file: {Path.GetFileName(filePath)}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"[{Name}] Failed to delete processed file '{filePath}': {ex.Message}");
            }
        }

        #region --- Helper Methods (Parsing, DB, File IO) ---

        private Dictionary<string, string> ParseMetadata(string[] lines)
        {
            var meta = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var ln in lines)
            {
                int idx = ln.IndexOf(':');
                if (idx > 0)
                {
                    string key = ln.Substring(0, idx).Trim();
                    string val = ln.Substring(idx + 1).Trim();
                    if (!meta.ContainsKey(key))
                    {
                        meta[key] = val;
                        _logger.LogDebug($"[{Name}] Parsed metadata: '{key}' = '{val}'");
                    }
                }
            }
            return meta;
        }

        private (int? waferNo, DateTime dateTime) ExtractPrimaryMeta(Dictionary<string, string> meta)
        {
            int? waferNo = null;
            if (meta.TryGetValue("Wafer ID", out string waferId))
            {
                var m = Regex.Match(waferId, @"W(\d+)");
                if (m.Success && int.TryParse(m.Groups[1].Value, out int w))
                {
                    waferNo = w;
                }
            }
            DateTime.TryParse(meta.GetValueOrDefault("Date and Time"), CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dtVal);
            _logger.LogDebug($"[{Name}] Extracted primary metadata: WaferNo='{waferNo}', DateTime='{dtVal:s}'");
            return (waferNo, dtVal);
        }

        private int FindHeaderIndex(string[] lines)
        {
            return Array.FindIndex(lines, l => l.TrimStart().StartsWith("Point#", StringComparison.OrdinalIgnoreCase));
        }

        private List<Dictionary<string, object>> ParseDataRows(string[] lines, int headerIndex, Dictionary<string, string> meta, int? waferNo, DateTime dateTime)
        {
            string NormalizeHeader(string h)
            {
                h = h.ToLowerInvariant();
                h = Regex.Replace(h, @"\(\s*no\s*cal\.?\s*\)", " nocal ", RegexOptions.IgnoreCase);
                h = Regex.Replace(h, @"\bno[\s_]*cal\b", "nocal", RegexOptions.IgnoreCase);
                h = Regex.Replace(h, @"\(\s*cal\.?\s*\)", " cal ", RegexOptions.IgnoreCase);
                h = h.Replace("(mm)", "").Replace("(탆)", "").Replace("die x", "diex").Replace("die y", "diey").Trim();
                h = Regex.Replace(h, @"\s+", "_");
                h = Regex.Replace(h, @"[#/:\-]", "");
                return h;
            }

            var headers = lines[headerIndex].Split(',').Select(NormalizeHeader).ToList();
            _logger.LogDebug($"[{Name}] Normalized headers: {string.Join(", ", headers)}");
            var headerMap = headers.Select((h, i) => new { h, i }).GroupBy(x => x.h).ToDictionary(g => g.Key, g => g.First().i);

            var rows = new List<Dictionary<string, object>>();
            var intCols = new HashSet<string> { "point", "dierow", "diecol", "dienum", "diepointtag" };

            for (int i = headerIndex + 1; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i])) continue;
                var values = lines[i].Split(',').Select(v => v.Trim()).ToArray();
                if (values.Length < headers.Count)
                {
                    _logger.LogDebug($"[{Name}] Skipping line {i+1} due to insufficient column count ({values.Length}/{headers.Count}).");
                    continue;
                }

                var row = new Dictionary<string, object>
                {
                    ["cassettercp"] = meta.GetValueOrDefault("Cassette Recipe Name"),
                    ["stagercp"] = meta.GetValueOrDefault("Stage Recipe Name"),
                    ["stagegroup"] = meta.GetValueOrDefault("Stage Group Name"),
                    ["lotid"] = meta.GetValueOrDefault("Lot ID"),
                    ["waferid"] = (object)waferNo ?? DBNull.Value,
                    ["datetime"] = dateTime == DateTime.MinValue ? (object)DBNull.Value : dateTime,
                    ["film"] = meta.GetValueOrDefault("Film Name")
                };

                foreach (var h in headerMap)
                {
                    string val = (h.Value < values.Length) ? values[h.Value] : "";
                    if (string.IsNullOrEmpty(val))
                    {
                        row[h.Key] = DBNull.Value;
                    }
                    else if (intCols.Contains(h.Key) && int.TryParse(val, out int intVal))
                    {
                        row[h.Key] = intVal;
                    }
                    else if (double.TryParse(val, out double dblVal))
                    {
                        row[h.Key] = dblVal;
                    }
                    else
                    {
                        row[h.Key] = val;
                    }
                }
                rows.Add(row);
            }
            return rows;
        }

        private DataTable BuildDataTable(List<Dictionary<string, object>> rows, string eqpid)
        {
            var dt = new DataTable();
            if (rows.Count == 0) return dt;

            var allKeys = rows.SelectMany(r => r.Keys).Distinct().ToList();
            foreach (var key in allKeys)
            {
                // DB 테이블 컬럼과 이름/타입을 맞추는 것이 중요
                dt.Columns.Add(key, typeof(object));
            }
            dt.Columns.Add("eqpid", typeof(string));

            foreach (var row in rows)
            {
                var dr = dt.NewRow();
                foreach (var key in allKeys)
                {
                    dr[key] = row.GetValueOrDefault(key) ?? DBNull.Value;
                }
                dr["eqpid"] = eqpid;
                dt.Rows.Add(dr);
            }
            return dt;
        }

        private void UploadToDatabase(DataTable dt, string sourceFileName)
        {
            if (dt.Rows.Count == 0) return;

            // 서버 시간(serv_ts) 컬럼 추가 및 값 계산
            if (!dt.Columns.Contains("serv_ts"))
                dt.Columns.Add("serv_ts", typeof(DateTime));

            foreach (DataRow r in dt.Rows)
            {
                if (r["datetime"] != DBNull.Value && r["datetime"] is DateTime ts)
                {
                    var kstTime = _timeSync.ToSynchronizedKst(ts);
                    r["serv_ts"] = new DateTime(kstTime.Year, kstTime.Month, kstTime.Day, kstTime.Hour, kstTime.Minute, kstTime.Second);
                }
                else
                {
                    r["serv_ts"] = DBNull.Value;
                }
            }

            string connString = DatabaseInfo.CreateDefault().GetConnectionString();
            _logger.LogDebug($"[{Name}] Starting DB upload of {dt.Rows.Count} rows from '{sourceFileName}' to 'plg_wf_flat'.");
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                _logger.LogDebug($"[{Name}] Database connection opened.");
                using (var tx = conn.BeginTransaction())
                {
                    var allColumns = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                    string colList = string.Join(",", allColumns.Select(c => $"\"{c}\""));
                    string paramList = string.Join(",", allColumns.Select(c => "@" + c));
                    string sql = $"INSERT INTO public.plg_wf_flat ({colList}) VALUES ({paramList}) ON CONFLICT DO NOTHING;";

                    try
                    {
                        int successCount = 0;
                        for(int i = 0; i < dt.Rows.Count; i++)
                        {
                            using (var cmd = new NpgsqlCommand(sql, conn, tx))
                            {
                                foreach (var colName in allColumns)
                                {
                                    cmd.Parameters.AddWithValue("@" + colName, dt.Rows[i][colName] ?? DBNull.Value);
                                }
                                successCount += cmd.ExecuteNonQuery();
                            }
                        }
                        tx.Commit();
                        _logger.LogEvent($"[{Name}] Successfully uploaded {successCount} of {dt.Rows.Count} rows from '{sourceFileName}'.");
                        SimpleLogger.Event($"DB OK ▶ {successCount} rows from {sourceFileName}");
                    }
                    catch (Exception ex)
                    {
                        tx.Rollback();
                        _logger.LogError($"[{Name}] DB upload failed for '{sourceFileName}': {ex.Message}");
                        _logger.LogDebug($"[{Name}] DB upload exception details: {ex.ToString()}");
                        SimpleLogger.Error($"DB FAIL ▶ {ex.ToString()}");
                    }
                }
            }
        }

        private bool WaitForFileReady(string path, int maxRetries, int delayMs)
        {
            for (int i = 0; i < maxRetries; i++)
            {
                try
                {
                    using (File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) { return true; }
                }
                catch (IOException) { Thread.Sleep(delayMs); }
            }
            return false;
        }

        private string ReadAllTextSafe(string path, Encoding enc, int timeoutMs = 30000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (true)
            {
                try { return File.ReadAllText(path, enc); }
                catch (IOException)
                {
                    if (sw.ElapsedMilliseconds > timeoutMs)
                    {
                        _logger.LogError($"[{Name}] Could not read file '{path}' within {timeoutMs}ms timeout.");
                        throw new TimeoutException($"Could not read file {path} within {timeoutMs}ms.");
                    }
                    Thread.Sleep(250);
                }
            }
        }

        #endregion
    }

    internal static class SimpleLogger
    {
        private static volatile bool _debugEnabled = false;
        public static void SetDebugMode(bool enable) => _debugEnabled = enable;
        private static readonly object _sync = new object();
        private static readonly string _logDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");

        private static void Write(string suffix, string msg)
        {
            lock (_sync)
            {
                try
                {
                    Directory.CreateDirectory(_logDir);
                    string filePath = Path.Combine(_logDir, $"{DateTime.Now:yyyyMMdd}_{suffix}.log");
                    string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [Onto_WaferFlatData] {msg}{Environment.NewLine}";
                    File.AppendAllText(filePath, line, Encoding.UTF8);
                }
                catch { /* 로깅 실패는 무시 */ }
            }
        }
        public static void Event(string msg) => Write("event", msg);
        public static void Error(string msg) => Write("error", msg);
        public static void Debug(string msg)
        {
            if (_debugEnabled) Write("debug", msg);
        }
    }

    public static class DictionaryExtensions
    {
        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key)
        {
            return dict.TryGetValue(key, out TValue val) ? val : default(TValue);
        }
    }
}
