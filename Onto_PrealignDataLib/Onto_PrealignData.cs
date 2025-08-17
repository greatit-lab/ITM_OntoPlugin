// Onto_PrealignDataLib/Onto_PrealignData.cs
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

namespace Onto_PrealignDataLib
{
    /// <summary>
    /// Pre-Align 데이터를 파싱하여 데이터베이스에 업로드하는 플러그인입니다.
    /// </summary>
    public class Onto_PrealignData : IPlugin
    {
        private ISettingsManager _settings;
        private ILogManager _logger;
        private ITimeSyncProvider _timeSync;

        public string Name => "Onto_PrealignData";
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
            catch (InvalidOperationException) { /* 이미 등록됨 */ }
        }

        public void SetDebugMode(bool isEnabled)
        {
            SimpleLogger.SetDebugMode(isEnabled);
        }

        /// <summary>
        /// 플러그인의 주 실행 로직입니다. 파일 전체를 처리합니다.
        /// (원본의 ProcessAndUpload 역할)
        /// </summary>
        public void Execute(string filePath)
        {
            _logger.LogEvent($"[{Name}] Processing file: {Path.GetFileName(filePath)}");

            if (!WaitForFileReady(filePath, 10, 300))
            {
                _logger.LogEvent($"[{Name}] SKIPPED (file locked): {Path.GetFileName(filePath)}");
                return;
            }

            try
            {
                ProcessFullFile(filePath);
            }
            catch (Exception ex)
            {
                _logger.LogError($"[{Name}] Unhandled exception for {filePath}: {ex.Message}");
                SimpleLogger.Error($"Unhandled EXCEPTION: {ex.ToString()}");
            }
        }

        /// <summary>
        /// 파일 전체를 읽어 파싱하고 DB에 업로드합니다.
        /// </summary>
        private void ProcessFullFile(string filePath)
        {
            string eqpid = _settings.GetEqpid();
            string content = ReadAllTextSafe(filePath, Encoding.GetEncoding(949));
            var lines = content.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);

            var regex = new Regex(@"Xmm\s*([-\d.]+)\s*Ymm\s*([-\d.]+)\s*Notch\s*([-\d.]+)\s*Time\s*([\d\-:\s]+)", RegexOptions.IgnoreCase);
            var rows = new List<(decimal x, decimal y, decimal notch, DateTime timestamp)>();

            foreach (var line in lines)
            {
                Match m = regex.Match(line);
                if (!m.Success) continue;

                if (TryParseTimestamp(m.Groups[4].Value, out DateTime ts) &&
                    decimal.TryParse(m.Groups[1].Value, out decimal x) &&
                    decimal.TryParse(m.Groups[2].Value, out decimal y) &&
                    decimal.TryParse(m.Groups[3].Value, out decimal n))
                {
                    rows.Add((x, y, n, ts));
                }
            }

            if (rows.Count > 0)
            {
                InsertRows(rows, eqpid);
            }
            else
            {
                _logger.LogEvent($"[{Name}] No valid data rows found in {Path.GetFileName(filePath)}.");
            }
        }

        /// <summary>
        /// 파싱된 데이터 행들을 데이터베이스에 삽입합니다.
        /// </summary>
        private void InsertRows(List<(decimal x, decimal y, decimal notch, DateTime timestamp)> rows, string eqpid)
        {
            var dt = new DataTable();
            dt.Columns.Add("eqpid", typeof(string));
            dt.Columns.Add("datetime", typeof(DateTime));
            dt.Columns.Add("xmm", typeof(decimal));
            dt.Columns.Add("ymm", typeof(decimal));
            dt.Columns.Add("notch", typeof(decimal));
            dt.Columns.Add("serv_ts", typeof(DateTime));

            foreach (var row in rows)
            {
                var kstTime = _timeSync.ToSynchronizedKst(row.timestamp);
                var servTsWithoutMs = new DateTime(kstTime.Year, kstTime.Month, kstTime.Day, kstTime.Hour, kstTime.Minute, kstTime.Second);
                dt.Rows.Add(eqpid, row.timestamp, row.x, row.y, row.notch, servTsWithoutMs);
            }

            UploadDataTable(dt, "plg_prealign");
        }

        #region --- Helper Methods ---

        private void UploadDataTable(DataTable dt, string tableName)
        {
            string connString = DatabaseInfo.CreateDefault().GetConnectionString();
            try
            {
                using (var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    using (var tx = conn.BeginTransaction())
                    {
                        var allColumns = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                        string colList = string.Join(",", allColumns.Select(c => $"\"{c}\""));
                        string paramList = string.Join(",", allColumns.Select(c => "@" + c));
                        string sql = $"INSERT INTO public.{tableName} ({colList}) VALUES ({paramList}) ON CONFLICT (eqpid, datetime) DO NOTHING;";

                        int successCount = 0;
                        foreach (DataRow row in dt.Rows)
                        {
                            using (var cmd = new NpgsqlCommand(sql, conn, tx))
                            {
                                foreach (var colName in allColumns)
                                {
                                    cmd.Parameters.AddWithValue("@" + colName, row[colName] ?? DBNull.Value);
                                }
                                successCount += cmd.ExecuteNonQuery();
                            }
                        }
                        tx.Commit();
                        _logger.LogEvent($"[{Name}] Successfully uploaded {successCount} of {dt.Rows.Count} rows to {tableName}.");
                        SimpleLogger.Event($"DB OK ▶ {successCount} rows to {tableName}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"[{Name}] DB upload failed for {tableName}: {ex.Message}");
                SimpleLogger.Error($"DB FAIL ({tableName}) ▶ {ex.ToString()}");
            }
        }

        private bool TryParseTimestamp(string timeString, out DateTime timestamp)
        {
            string[] formats = { "MM-dd-yy HH:mm:ss", "M-d-yy HH:mm:ss" };
            if (DateTime.TryParseExact(timeString.Trim(), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out timestamp))
            {
                return true;
            }
            // 형식이 맞지 않으면 일반 TryParse도 시도 (원본 로직 유지)
            return DateTime.TryParse(timeString.Trim(), out timestamp);
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
                    if (sw.ElapsedMilliseconds > timeoutMs) throw new TimeoutException($"Could not read file {path} within {timeoutMs}ms.");
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
                    string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [Onto_PrealignData] {msg}{Environment.NewLine}";
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
}
