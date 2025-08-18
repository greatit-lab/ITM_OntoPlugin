// Onto_ErrorDataLib/Onto_ErrorData.cs
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

namespace Onto_ErrorDataLib
{
    /// <summary>
    /// Error 로그 파일을 파싱하여 필터링 후 데이터베이스에 업로드하는 플러그인입니다.
    /// </summary>
    public class Onto_ErrorData : IPlugin
    {
        private ISettingsManager _settings;
        private ILogManager _logger;
        private ITimeSyncProvider _timeSync;

        public string Name => "Onto_ErrorData";
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

        public void Execute(string filePath)
        {
            _logger.LogEvent($"[{Name}] Processing file: {Path.GetFileName(filePath)}");

            if (!WaitForFileReady(filePath, 20, 500))
            {
                _logger.LogEvent($"[{Name}] SKIPPED (file locked): {Path.GetFileName(filePath)}");
                return;
            }

            try
            {
                ProcessFile(filePath);
            }
            catch (Exception ex)
            {
                _logger.LogError($"[{Name}] Unhandled exception for {filePath}: {ex.Message}");
                SimpleLogger.Error($"Unhandled EXCEPTION: {ex.ToString()}");
            }
        }

        private void ProcessFile(string filePath)
        {
            string eqpid = _settings.GetEqpid();
            string fileContent = ReadAllTextSafe(filePath, Encoding.GetEncoding(949));
            var lines = fileContent.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);

            var meta = ParseMetadata(lines);
            if (!meta.ContainsKey("EqpId")) meta["EqpId"] = eqpid;

            var infoTable = BuildInfoDataTable(meta);
            UploadItmInfo(infoTable);

            var errorTable = BuildErrorDataTable(lines, eqpid);
            HashSet<string> allowedErrorIds = LoadAllowedErrorIds();
            var (filteredTable, matched, skipped) = ApplyErrorFilter(errorTable, allowedErrorIds);

            _logger.LogEvent($"[{Name}] ErrorFilter Result for {Path.GetFileName(filePath)}: Total={errorTable.Rows.Count}, Matched={matched}, Skipped={skipped}");

            if (filteredTable.Rows.Count > 0)
            {
                UploadDataTable(filteredTable, "plg_error");
            }
            else
            {
                _logger.LogEvent($"[{Name}] No rows to upload after filtering for plg_error.");
            }
        }

        #region --- Helper Methods (Parsing, DB, File IO) ---

        private Dictionary<string, string> ParseMetadata(string[] lines)
        {
            var meta = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in lines)
            {
                int idx = line.IndexOf(":,");
                if (idx > 0)
                {
                    string key = line.Substring(0, idx).Trim();
                    string val = line.Substring(idx + 2).Trim();
                    if (key.Length > 0 && !key.Equals("EXPORT_TYPE", StringComparison.OrdinalIgnoreCase))
                    {
                        meta[key] = val;
                    }
                }
            }

            if (meta.TryGetValue("DATE", out string dateStr) &&
                DateTime.TryParseExact(dateStr, "M/d/yyyy H:m:s", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate))
            {
                meta["DATE"] = parsedDate.ToString("yyyy-MM-dd HH:mm:ss");
            }
            return meta;
        }

        private DataTable BuildInfoDataTable(Dictionary<string, string> meta)
        {
            var dt = new DataTable();
            dt.Columns.Add("eqpid", typeof(string));
            dt.Columns.Add("system_name", typeof(string));
            dt.Columns.Add("system_model", typeof(string));
            dt.Columns.Add("serial_num", typeof(string));
            dt.Columns.Add("application", typeof(string));
            dt.Columns.Add("version", typeof(string));
            dt.Columns.Add("db_version", typeof(string));
            dt.Columns.Add("date", typeof(string));

            var row = dt.NewRow();
            row["eqpid"] = meta.GetValueOrDefault("EqpId");
            row["system_name"] = meta.GetValueOrDefault("SYSTEM_NAME");
            row["system_model"] = meta.GetValueOrDefault("SYSTEM_MODEL");
            row["serial_num"] = meta.GetValueOrDefault("SERIAL_NUM");
            row["application"] = meta.GetValueOrDefault("APPLICATION");
            row["version"] = meta.GetValueOrDefault("VERSION");
            row["db_version"] = meta.GetValueOrDefault("DB_VERSION");
            row["date"] = meta.GetValueOrDefault("DATE");
            dt.Rows.Add(row);
            return dt;
        }

        private DataTable BuildErrorDataTable(string[] lines, string eqpid)
        {
            var dt = new DataTable();
            dt.Columns.AddRange(new[]
            {
                new DataColumn("eqpid", typeof(string)),
                new DataColumn("error_id", typeof(string)),
                new DataColumn("time_stamp", typeof(DateTime)),
                new DataColumn("error_label", typeof(string)),
                new DataColumn("error_desc", typeof(string)),
                new DataColumn("millisecond", typeof(int)),
                new DataColumn("extra_message_1", typeof(string)),
                new DataColumn("extra_message_2", typeof(string)),
                new DataColumn("serv_ts", typeof(DateTime))
            });

            var regex = new Regex(@"^(?<id>\w+),\s*(?<ts>[^,]+),\s*(?<lbl>[^,]+),\s*(?<desc>[^,]+),\s*(?<ms>\d+)(?:,\s*(?<extra>.*))?", RegexOptions.Compiled);

            foreach (var line in lines)
            {
                var m = regex.Match(line);
                if (!m.Success) continue;

                var dr = dt.NewRow();
                dr["eqpid"] = eqpid;
                dr["error_id"] = m.Groups["id"].Value.Trim();

                if (DateTime.TryParseExact(m.Groups["ts"].Value.Trim(), "dd-MMM-yy h:mm:ss tt", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedTs))
                {
                    dr["time_stamp"] = parsedTs;
                    var kstTime = _timeSync.ToSynchronizedKst(parsedTs);
                    dr["serv_ts"] = new DateTime(kstTime.Year, kstTime.Month, kstTime.Day, kstTime.Hour, kstTime.Minute, kstTime.Second);
                }

                dr["error_label"] = m.Groups["lbl"].Value.Trim();
                dr["error_desc"] = m.Groups["desc"].Value.Trim();
                if (int.TryParse(m.Groups["ms"].Value, out int ms)) dr["millisecond"] = ms;
                dr["extra_message_1"] = m.Groups["extra"].Value.Trim();
                dr["extra_message_2"] = "";

                dt.Rows.Add(dr);
            }
            return dt;
        }

        private HashSet<string> LoadAllowedErrorIds()
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string connString = DatabaseInfo.CreateDefault().GetConnectionString();
            try
            {
                using (var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    using (var cmd = new NpgsqlCommand("SELECT error_id FROM public.err_severity_map", conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (!reader.IsDBNull(0))
                            {
                                set.Add(reader.GetString(0).Trim());
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"[{Name}] Failed to load allowed error IDs from DB: {ex.Message}");
                SimpleLogger.Error($"DB FAIL (LoadAllowedErrorIds) ▶ {ex.ToString()}");
            }
            return set;
        }

        private (DataTable filteredTable, int matched, int skipped) ApplyErrorFilter(DataTable source, HashSet<string> allowSet)
        {
            if (source == null || source.Rows.Count == 0 || allowSet == null || allowSet.Count == 0)
            {
                return (source?.Clone() ?? new DataTable(), 0, source?.Rows.Count ?? 0);
            }

            var destination = source.Clone();
            int matched = 0;
            foreach (DataRow row in source.Rows)
            {
                string errorId = row["error_id"]?.ToString()?.Trim() ?? "";
                if (allowSet.Any(id => id.Equals(errorId, StringComparison.OrdinalIgnoreCase)))
                {
                    destination.ImportRow(row);
                    matched++;
                }
            }
            return (destination, matched, source.Rows.Count - matched);
        }

        private void UploadItmInfo(DataTable dt)
        {
            if (dt == null || dt.Rows.Count == 0) return;
            var row = dt.Rows[0];
            string connString = DatabaseInfo.CreateDefault().GetConnectionString();

            if (IsInfoUnchanged(row))
            {
                SimpleLogger.Event("itm_info unchanged ▶ eqpid=" + (row["eqpid"] ?? ""));
                return;
            }

            DateTime.TryParseExact(row["date"]?.ToString(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate);
            var servTs = _timeSync.ToSynchronizedKst(parsedDate == DateTime.MinValue ? DateTime.Now : parsedDate);
            var servTsWithoutMs = new DateTime(servTs.Year, servTs.Month, servTs.Day, servTs.Hour, servTs.Minute, servTs.Second);

            const string sql = @"
                INSERT INTO public.itm_info (eqpid, system_name, system_model, serial_num, application, version, db_version, ""date"", serv_ts)
                VALUES (@eqpid, @system_name, @system_model, @serial_num, @application, @version, @db_version, @date, @serv_ts)
                ON CONFLICT (eqpid) DO UPDATE SET
                    system_name = EXCLUDED.system_name, system_model = EXCLUDED.system_model, serial_num = EXCLUDED.serial_num,
                    application = EXCLUDED.application, version = EXCLUDED.version, db_version = EXCLUDED.db_version,
                    ""date"" = EXCLUDED.date, serv_ts = EXCLUDED.serv_ts;";

            try
            {
                using (var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    using (var cmd = new NpgsqlCommand(sql, conn))
                    {
                        cmd.Parameters.AddWithValue("@eqpid", row["eqpid"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@system_name", row["system_name"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@system_model", row["system_model"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@serial_num", row["serial_num"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@application", row["application"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@version", row["version"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@db_version", row["db_version"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@date", parsedDate == DateTime.MinValue ? (object)DBNull.Value : parsedDate);
                        cmd.Parameters.AddWithValue("@serv_ts", servTsWithoutMs);
                        cmd.ExecuteNonQuery();
                    }
                }
                _logger.LogEvent($"[{Name}] itm_info table updated for eqpid: {row["eqpid"]}");
                SimpleLogger.Event($"itm_info inserted/updated ▶ eqpid={row["eqpid"]}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"[{Name}] DB upload failed for itm_info: {ex.Message}");
                SimpleLogger.Error($"DB FAIL (UploadItmInfo) ▶ {ex.ToString()}");
            }
        }

        private bool IsInfoUnchanged(DataRow r)
        {
            string connString = DatabaseInfo.CreateDefault().GetConnectionString();
            const string sql = @"
                SELECT 1 FROM public.itm_info WHERE eqpid = @eqp
                  AND system_name IS NOT DISTINCT FROM @sn AND system_model IS NOT DISTINCT FROM @sm
                  AND serial_num IS NOT DISTINCT FROM @snm AND application IS NOT DISTINCT FROM @app
                  AND version IS NOT DISTINCT FROM @ver AND db_version IS NOT DISTINCT FROM @dbv
                LIMIT 1;";

            try
            {
                using (var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    using (var cmd = new NpgsqlCommand(sql, conn))
                    {
                        cmd.Parameters.AddWithValue("@eqp", r["eqpid"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@sn", r["system_name"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@sm", r["system_model"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@snm", r["serial_num"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@app", r["application"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@ver", r["version"] ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@dbv", r["db_version"] ?? DBNull.Value);
                        return cmd.ExecuteScalar() != null;
                    }
                }
            }
            catch { return false; }
        }

        private void UploadDataTable(DataTable dt, string tableName)
        {
            string connString = DatabaseInfo.CreateDefault().GetConnectionString();
            try
            {
                using (var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();
                    var allColumns = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                    string colList = string.Join(",", allColumns.Select(c => $"\"{c}\""));
                    string copyCommand = $"COPY public.{tableName} ({colList}) FROM STDIN (FORMAT BINARY)";

                    using (var writer = conn.BeginBinaryImport(copyCommand))
                    {
                        foreach (DataRow row in dt.Rows)
                        {
                            writer.StartRow();
                            foreach (var colName in allColumns)
                            {
                                var npgsqlType = GetNpgsqlDbType(dt.Columns[colName].DataType);
                                writer.Write(row[colName], npgsqlType);
                            }
                        }
                        writer.Complete();
                    }
                }
                _logger.LogEvent($"[{Name}] Successfully uploaded {dt.Rows.Count} rows to {tableName}.");
                SimpleLogger.Event($"DB OK ▶ {dt.Rows.Count} rows to {tableName}");
            }
            catch (PostgresException pex) when (pex.SqlState == "23505")
            {
                _logger.LogEvent($"[{Name}] Skipping duplicate entries for {tableName}.");
                SimpleLogger.Event($"Duplicate entry skipped ▶ {tableName}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"[{Name}] DB upload failed for {tableName}: {ex.Message}");
                SimpleLogger.Error($"DB FAIL ({tableName}) ▶ {ex.ToString()}");
            }
        }

        private NpgsqlTypes.NpgsqlDbType GetNpgsqlDbType(Type type)
        {
            if (type == typeof(string)) return NpgsqlTypes.NpgsqlDbType.Varchar;
            if (type == typeof(int)) return NpgsqlTypes.NpgsqlDbType.Integer;
            if (type == typeof(DateTime)) return NpgsqlTypes.NpgsqlDbType.Timestamp;
            if (type == typeof(double)) return NpgsqlTypes.NpgsqlDbType.Double;
            if (type == typeof(long)) return NpgsqlTypes.NpgsqlDbType.Bigint;
            if (type == typeof(bool)) return NpgsqlTypes.NpgsqlDbType.Boolean;
            return NpgsqlTypes.NpgsqlDbType.Unknown;
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
                    string line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [Onto_ErrorData] {msg}{Environment.NewLine}";
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
