// Сервис транскрибации звонков.
//
// Pipeline:
//
//	Kafka 'calls-ready'
//	  → скачать MP3 из MinIO
//	  → POST аудио в whisperx-service
//	  → обновить PostgreSQL (transcript_text, status='transcribed')
//	  → отправить в Kafka 'transcriptions-done'
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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

// ── структуры событий ───────────────────────────────────────────

type CallReadyEvent struct {
	CallID       string `json:"call_id"`
	FilePath     string `json:"file_path"`
	Source       string `json:"source"`
	EmployeeID   string `json:"employee_id"`
	EmployeeName string `json:"employee_name"`
	PhoneFrom    string `json:"phone_from"`
	PhoneTo      string `json:"phone_to"`
	Direction    string `json:"direction"`
	Duration     int    `json:"duration"`
	SizeBytes    int    `json:"size_bytes"`
	UploadedAt   string `json:"uploaded_at"`
}

type TranscriptResult struct {
	Language            string           `json:"language"`
	LanguageProbability float64          `json:"language_probability"`
	Segments            []map[string]any `json:"segments"`
	FullText            string           `json:"full_text"`
}

// ── PostgreSQL helpers ──────────────────────────────────────────

func logDB(ctx context.Context, pool *pgxpool.Pool, callID, level, stage, message string) {
	var callIDArg any
	if callID != "" {
		callIDArg = callID
	}
	_, err := pool.Exec(ctx,
		`INSERT INTO call_logs (call_id, level, stage, message) VALUES ($1, $2, $3, $4)`,
		callIDArg, level, stage, message,
	)
	if err != nil {
		slog.Warn("не удалось записать лог в БД", "err", err)
	}
}

// ── WhisperX HTTP вызов ─────────────────────────────────────────

func callWhisperX(ctx context.Context, httpClient *http.Client, whisperURL string, audioBytes []byte, language string) (*TranscriptResult, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	fw, err := w.CreateFormFile("file", "audio.mp3")
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(audioBytes); err != nil {
		return nil, err
	}
	if err := w.WriteField("language", language); err != nil {
		return nil, err
	}
	w.Close()

	req, err := http.NewRequestWithContext(ctx, "POST", whisperURL+"/transcribe", &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("whisperx %d: %s", resp.StatusCode, body)
	}

	var result TranscriptResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return &result, nil
}

// ── обработка одного события ────────────────────────────────────

func processRecord(
	ctx context.Context,
	record *kgo.Record,
	httpClient *http.Client,
	whisperURL string,
	whisperLang string,
	mc *minio.Client,
	bucket string,
	kClient *kgo.Client,
	pool *pgxpool.Pool,
) error {
	var event CallReadyEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("parse event: %w", err)
	}

	callID := event.CallID
	if callID == "" {
		callID = "unknown"
	}
	slog.Info("транскрибирую", "call_id", callID)

	// Скачиваем аудио из MinIO
	obj, err := mc.GetObject(ctx, bucket, event.FilePath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("minio get: %w", err)
	}
	defer obj.Close()

	audioBytes, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("read audio: %w", err)
	}

	// Транскрибируем через whisperx-service
	transcript, err := callWhisperX(ctx, httpClient, whisperURL, audioBytes, whisperLang)
	if err != nil {
		return fmt.Errorf("whisperx: %w", err)
	}

	// Сериализуем сегменты для JSONB
	segmentsJSON, _ := json.Marshal(transcript.Segments)

	// Обновляем PostgreSQL
	_, err = pool.Exec(ctx,
		`UPDATE calls SET
		     status = 'transcribed',
		     transcript_text = $1,
		     transcript_language = $2,
		     transcript_language_probability = $3,
		     transcript_segments = $4,
		     transcribed_at = NOW()
		 WHERE call_id = $5`,
		transcript.FullText,
		transcript.Language,
		transcript.LanguageProbability,
		segmentsJSON,
		callID,
	)
	if err != nil {
		slog.Warn("db update transcript", "call_id", callID, "err", err)
	} else {
		logDB(ctx, pool, callID, "info", "transcription",
			fmt.Sprintf("Транскрибировано: %d сегментов", len(transcript.Segments)))
	}

	// Отправляем событие в Kafka
	outEvent := map[string]any{
		"call_id":       event.CallID,
		"file_path":     event.FilePath,
		"source":        event.Source,
		"employee_id":   event.EmployeeID,
		"employee_name": event.EmployeeName,
		"phone_from":    event.PhoneFrom,
		"phone_to":      event.PhoneTo,
		"direction":     event.Direction,
		"duration":      event.Duration,
		"uploaded_at":   event.UploadedAt,
		"transcript":    transcript,
	}
	outJSON, _ := json.Marshal(outEvent)
	if err := kClient.ProduceSync(ctx, &kgo.Record{
		Topic: "transcriptions-done",
		Value: outJSON,
	}).FirstErr(); err != nil {
		slog.Warn("kafka produce transcriptions-done", "call_id", callID, "err", err)
	}

	slog.Info("готово", "call_id", callID,
		"segments", len(transcript.Segments),
		"language", transcript.Language,
		"probability", transcript.LanguageProbability,
	)
	return nil
}

// ── main ────────────────────────────────────────────────────────

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	kafkaBootstrap := getenv("KAFKA_BOOTSTRAP", "kafka:9092")
	minioEndpoint := mustenv("MINIO_ENDPOINT")
	minioAccess := getenv("MINIO_ACCESS_KEY", "minioadmin")
	minioSecret := getenv("MINIO_SECRET_KEY", "minioadmin")
	minioBucket := getenv("MINIO_BUCKET", "call-audios")
	postgresDSN := mustenv("POSTGRES_DSN")
	whisperURL := getenv("WHISPERX_URL", "http://whisperx-service:8001")
	whisperLang := getenv("WHISPER_LANGUAGE", "ru")

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

	// Kafka consumer + producer
	kClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaBootstrap, ",")...),
		kgo.ConsumerGroup("transcriber-group"),
		kgo.ConsumeTopics("calls-ready"),
		kgo.DisableAutoCommit(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.SessionTimeout(30*time.Second),
		kgo.HeartbeatInterval(10*time.Second),
	)
	if err != nil {
		slog.Error("kafka connect", "err", err)
		os.Exit(1)
	}
	defer kClient.Close()

	// Ждём Kafka
	for i := range 20 {
		if err := kClient.Ping(ctx); err == nil {
			slog.Info("kafka готова")
			break
		}
		slog.Warn("kafka ожидание", "attempt", i+1)
		time.Sleep(3 * time.Second)
	}

	// Ждём whisperx-service
	httpClient := &http.Client{Timeout: 600 * time.Second}
	for i := range 30 {
		resp, err := httpClient.Get(whisperURL + "/health")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			slog.Info("whisperx-service готов")
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		slog.Warn("whisperx-service ожидание", "attempt", i+1)
		time.Sleep(5 * time.Second)
	}

	slog.Info("слушаю топик 'calls-ready'...")

	for {
		fetches := kClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				slog.Error("kafka fetch error", "err", e.Err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			err := processRecord(ctx, record, httpClient, whisperURL, whisperLang, mc, minioBucket, kClient, pool)
			if err != nil {
				var callID string
				var ev CallReadyEvent
				if jsonErr := json.Unmarshal(record.Value, &ev); jsonErr == nil {
					callID = ev.CallID
				}
				slog.Error("ОШИБКА обработки", "call_id", callID, "err", err)

				if callID != "" {
					pool.Exec(ctx,
						`UPDATE calls SET status='failed_transcription' WHERE call_id=$1`,
						callID,
					)
					logDB(ctx, pool, callID, "error", "transcription", "Ошибка: "+err.Error())

					errEvent, _ := json.Marshal(map[string]any{
						"call_id": callID,
						"error":   err.Error(),
						"stage":   "transcription",
					})
					kClient.ProduceSync(ctx, &kgo.Record{
						Topic: "calls-errors",
						Value: errEvent,
					})
				}
			}

			// Коммитим оффсет после обработки
			kClient.CommitRecords(ctx, record)
		})
	}
}
