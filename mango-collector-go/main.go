// Сервис скачивания записей звонков из МангоОфис API.
//
// Pipeline каждые POLL_INTERVAL_SECONDS:
//  1. Повторить попытки для записей со статусом failed_download
//  2. Получить список новых записей из МангоОфис API
//  3. Для каждой новой записи:
//     - Создать запись в PostgreSQL (status=pending)
//     - Скачать MP3 из МангоОфис
//     - Загрузить в MinIO
//     - Обновить PostgreSQL (status=stored)
//     - Отправить событие в Kafka: calls-ready
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ── конфигурация ────────────────────────────────────────────────

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("обязательная переменная окружения не задана", "var", key)
		os.Exit(1)
	}
	return v
}

func getInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

var (
	mangoAPIURL    = getenv("MANGO_API_URL", "https://app.mango-office.ru/vpbx")
	mangoAPIKey    string
	mangoAPISalt   string
	pollInterval   time.Duration
	lookbackMins   int
	kafkaBootstrap string
	minioEndpoint  string
	minioAccess    string
	minioSecret    string
	minioBucket    string
	postgresDSN    string
)

// ── MangoOffice API ─────────────────────────────────────────────

func sign(jsonBody string) string {
	raw := mangoAPIKey + jsonBody + mangoAPISalt
	return fmt.Sprintf("%x", sha256.Sum256([]byte(raw)))
}

func mangoPost(ctx context.Context, client *http.Client, endpoint string, params map[string]any) (map[string]any, error) {
	jsonBytes, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	jsonBody := string(jsonBytes)

	form := url.Values{
		"vpbx_api_key": {mangoAPIKey},
		"sign":         {sign(jsonBody)},
		"json":         {jsonBody},
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		strings.TrimRight(mangoAPIURL, "/")+"/"+strings.TrimLeft(endpoint, "/"),
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("mango API %d: %s", resp.StatusCode, body)
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func downloadRecording(ctx context.Context, client *http.Client, recordingID string) ([]byte, error) {
	jsonBody, _ := json.Marshal(map[string]any{"recording_id": recordingID})
	bodyStr := string(jsonBody)

	form := url.Values{
		"vpbx_api_key": {mangoAPIKey},
		"sign":         {sign(bodyStr)},
		"json":         {bodyStr},
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		strings.TrimRight(mangoAPIURL, "/")+"/stats/calls/recording/file/",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("download %d: %s", resp.StatusCode, body)
	}

	return io.ReadAll(resp.Body)
}

// ── PostgreSQL helpers ──────────────────────────────────────────

func logDB(ctx context.Context, pool *pgxpool.Pool, callID, level, stage, message string) {
	_, err := pool.Exec(ctx,
		`INSERT INTO call_logs (call_id, level, stage, message) VALUES ($1, $2, $3, $4)`,
		nilIfEmpty(callID), level, stage, message,
	)
	if err != nil {
		slog.Warn("не удалось записать лог в БД", "err", err)
	}
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ── Загрузка и сохранение одной записи ─────────────────────────

type callMeta struct {
	EmployeeID   string
	EmployeeName string
	PhoneFrom    string
	PhoneTo      string
	Direction    string
	Duration     int
	StartTime    *time.Time
}

func uploadAndStore(
	ctx context.Context,
	callID, recordingID string,
	meta callMeta,
	httpClient *http.Client,
	mc *minio.Client,
	kClient *kgo.Client,
	pool *pgxpool.Pool,
) error {
	today := time.Now().UTC().Format("2006-01-02")
	objectKey := fmt.Sprintf("calls/%s/%s.mp3", today, callID)

	audioBytes, err := downloadRecording(ctx, httpClient, recordingID)
	if err != nil {
		return fmt.Errorf("скачивание: %w", err)
	}

	_, err = mc.PutObject(ctx, minioBucket, objectKey,
		bytes.NewReader(audioBytes), int64(len(audioBytes)),
		minio.PutObjectOptions{ContentType: "audio/mpeg"},
	)
	if err != nil {
		return fmt.Errorf("minio upload: %w", err)
	}

	_, err = pool.Exec(ctx,
		`UPDATE calls SET status='stored', file_path=$1, file_size_bytes=$2 WHERE call_id=$3`,
		objectKey, len(audioBytes), callID,
	)
	if err != nil {
		return fmt.Errorf("db update: %w", err)
	}

	event := map[string]any{
		"call_id":       callID,
		"file_path":     objectKey,
		"source":        "mango",
		"recording_id":  recordingID,
		"employee_id":   meta.EmployeeID,
		"employee_name": meta.EmployeeName,
		"phone_from":    meta.PhoneFrom,
		"phone_to":      meta.PhoneTo,
		"direction":     meta.Direction,
		"duration":      meta.Duration,
		"size_bytes":    len(audioBytes),
		"uploaded_at":   time.Now().UTC().Format(time.RFC3339),
	}
	if meta.StartTime != nil {
		event["start_time"] = meta.StartTime.Format(time.RFC3339)
	}

	eventJSON, _ := json.Marshal(event)
	if err := kClient.ProduceSync(ctx, &kgo.Record{
		Topic: "calls-ready",
		Value: eventJSON,
	}).FirstErr(); err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}

	logDB(ctx, pool, callID, "info", "download",
		fmt.Sprintf("Запись скачана и загружена (%d байт)", len(audioBytes)))
	slog.Info("загружено", "call_id", callID, "bytes", len(audioBytes))
	return nil
}

// ── Повторные попытки для failed_download ───────────────────────

func retryFailed(ctx context.Context, httpClient *http.Client, mc *minio.Client, kClient *kgo.Client, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx,
		`SELECT call_id, mango_recording_id,
		        employee_id, employee_name, phone_from, phone_to,
		        direction, duration_seconds, start_time
		 FROM calls
		 WHERE status = 'failed_download'
		   AND mango_recording_id IS NOT NULL
		   AND updated_at < NOW() - INTERVAL '5 minutes'
		 LIMIT 10`,
	)
	if err != nil {
		slog.Warn("retry query", "err", err)
		return
	}
	defer rows.Close()

	type retryRow struct {
		callID      string
		recordingID string
		meta        callMeta
	}
	var toRetry []retryRow

	for rows.Next() {
		var (
			callID, recordingID             string
			empID, empName, phoneFrom, phoneTo, direction *string
			durationSec                     *int
			startTime                       *time.Time
		)
		if err := rows.Scan(&callID, &recordingID,
			&empID, &empName, &phoneFrom, &phoneTo,
			&direction, &durationSec, &startTime,
		); err != nil {
			slog.Warn("retry scan", "err", err)
			continue
		}
		toRetry = append(toRetry, retryRow{
			callID:      callID,
			recordingID: recordingID,
			meta: callMeta{
				EmployeeID:   derefStr(empID),
				EmployeeName: derefStr(empName),
				PhoneFrom:    derefStr(phoneFrom),
				PhoneTo:      derefStr(phoneTo),
				Direction:    derefStr(direction),
				Duration:     derefInt(durationSec),
				StartTime:    startTime,
			},
		})
	}

	for _, r := range toRetry {
		slog.Info("повторная попытка скачивания", "call_id", r.callID)
		if err := uploadAndStore(ctx, r.callID, r.recordingID, r.meta, httpClient, mc, kClient, pool); err != nil {
			slog.Error("повторная попытка провалилась", "call_id", r.callID, "err", err)
			logDB(ctx, pool, r.callID, "error", "download", "Retry failed: "+err.Error())
		}
	}
}

// ── Основной цикл опроса ────────────────────────────────────────

func pollOnce(ctx context.Context, httpClient *http.Client, mc *minio.Client, kClient *kgo.Client, pool *pgxpool.Pool) {
	retryFailed(ctx, httpClient, mc, kClient, pool)

	now := time.Now().UTC()
	extraMin := int(pollInterval.Minutes()) + 1
	dateFrom := now.Add(-time.Duration(lookbackMins+extraMin) * time.Minute).Unix()
	dateTo := now.Unix()

	data, err := mangoPost(ctx, httpClient, "stats/calls/records/", map[string]any{
		"date_from": dateFrom,
		"date_to":   dateTo,
		"fields":    "start,finish,duration,from_number,to_number,is_recorded,recording,emp_id,emp_name,call_direction",
	})
	if err != nil {
		slog.Error("MangoOffice API ошибка", "err", err)
		logDB(ctx, pool, "", "error", "download", "MangoOffice API error: "+err.Error())
		return
	}

	results, _ := data["results"].([]any)
	newCount := 0

	for _, groupAny := range results {
		group, _ := groupAny.(map[string]any)
		records, _ := group["records"].([]any)

		for _, recAny := range records {
			record, _ := recAny.(map[string]any)
			recordingID, _ := record["recording"].(string)
			if recordingID == "" {
				continue
			}

			// Уже есть в БД?
			var existingCallID string
			err := pool.QueryRow(ctx,
				`SELECT call_id FROM calls WHERE mango_recording_id = $1 LIMIT 1`,
				recordingID,
			).Scan(&existingCallID)
			if err == nil {
				continue // уже обработали
			}
			if err != pgx.ErrNoRows {
				slog.Warn("db check", "err", err)
				continue
			}

			callID := "mango_" + recordingID
			direction := "outbound"
			if fmt.Sprint(record["call_direction"]) == "0" {
				direction = "inbound"
			}

			var startTime *time.Time
			if startTS, ok := record["start"].(float64); ok && startTS > 0 {
				t := time.Unix(int64(startTS), 0).UTC()
				startTime = &t
			}

			duration := 0
			if d, ok := record["duration"].(float64); ok {
				duration = int(d)
			}

			meta := callMeta{
				EmployeeID:   fmt.Sprint(record["emp_id"]),
				EmployeeName: fmt.Sprint(record["emp_name"]),
				PhoneFrom:    fmt.Sprint(record["from_number"]),
				PhoneTo:      fmt.Sprint(record["to_number"]),
				Direction:    direction,
				Duration:     duration,
				StartTime:    startTime,
			}

			// Создаём запись в БД
			_, err = pool.Exec(ctx,
				`INSERT INTO calls
				 (call_id, mango_recording_id, source,
				  employee_id, employee_name, phone_from, phone_to,
				  direction, duration_seconds, start_time, status)
				 VALUES ($1,$2,'mango',$3,$4,$5,$6,$7,$8,$9,'pending')
				 ON CONFLICT (call_id) DO NOTHING`,
				callID, recordingID,
				nilIfEmpty(meta.EmployeeID), nilIfEmpty(meta.EmployeeName),
				nilIfEmpty(meta.PhoneFrom), nilIfEmpty(meta.PhoneTo),
				meta.Direction, meta.Duration, startTime,
			)
			if err != nil {
				slog.Error("не удалось создать запись в БД", "call_id", callID, "err", err)
				continue
			}

			if err := uploadAndStore(ctx, callID, recordingID, meta, httpClient, mc, kClient, pool); err != nil {
				slog.Error("ошибка загрузки", "call_id", callID, "err", err)
				pool.Exec(ctx,
					`UPDATE calls SET status='failed_download' WHERE call_id=$1`,
					callID,
				)
				logDB(ctx, pool, callID, "error", "download", "Ошибка загрузки: "+err.Error())
				continue
			}
			newCount++
		}
	}

	if newCount > 0 {
		slog.Info("обработано новых записей", "count", newCount)
	} else {
		slog.Info("новых записей нет")
	}
}

// ── Вспомогательные функции ─────────────────────────────────────

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(n *int) int {
	if n == nil {
		return 0
	}
	return *n
}

// ── main ────────────────────────────────────────────────────────

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	mangoAPIKey = mustenv("MANGO_API_KEY")
	mangoAPISalt = mustenv("MANGO_API_SALT")
	kafkaBootstrap = getenv("KAFKA_BOOTSTRAP", "kafka:9092")
	minioEndpoint = mustenv("MINIO_ENDPOINT")
	minioAccess = getenv("MINIO_ACCESS_KEY", "minioadmin")
	minioSecret = getenv("MINIO_SECRET_KEY", "minioadmin")
	minioBucket = getenv("MINIO_BUCKET", "call-audios")
	postgresDSN = mustenv("POSTGRES_DSN")
	pollInterval = time.Duration(getInt("POLL_INTERVAL_SECONDS", 300)) * time.Second
	lookbackMins = getInt("LOOKBACK_MINUTES", 10)

	ctx := context.Background()

	// PostgreSQL
	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		slog.Error("postgres connect", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// MinIO
	mc, err := minio.New(
		strings.TrimPrefix(strings.TrimPrefix(minioEndpoint, "http://"), "https://"),
		&minio.Options{
			Creds:  credentials.NewStaticV4(minioAccess, minioSecret, ""),
			Secure: strings.HasPrefix(minioEndpoint, "https://"),
		},
	)
	if err != nil {
		slog.Error("minio connect", "err", err)
		os.Exit(1)
	}

	exists, err := mc.BucketExists(ctx, minioBucket)
	if err != nil || !exists {
		if mkErr := mc.MakeBucket(ctx, minioBucket, minio.MakeBucketOptions{}); mkErr != nil {
			slog.Warn("minio make bucket", "err", mkErr)
		}
	}

	// Kafka producer
	kClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaBootstrap, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RetryBackoffFn(func(n int) time.Duration { return time.Duration(n) * time.Second }),
	)
	if err != nil {
		slog.Error("kafka connect", "err", err)
		os.Exit(1)
	}
	defer kClient.Close()

	// Ждём Kafka
	for i := range 20 {
		if err := kClient.Ping(ctx); err == nil {
			break
		}
		slog.Warn("kafka ожидание", "attempt", i+1)
		time.Sleep(3 * time.Second)
	}

	httpClient := &http.Client{Timeout: 120 * time.Second}

	slog.Info("старт", "poll_interval", pollInterval, "lookback_minutes", lookbackMins)

	for {
		pollOnce(ctx, httpClient, mc, kClient, pool)
		time.Sleep(pollInterval)
	}
}
