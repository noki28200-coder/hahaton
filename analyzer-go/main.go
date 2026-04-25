// Сервис анализа транскрипций через Ollama/Gemma.
//
// Pipeline:
//
//	Kafka 'transcriptions-done'
//	  → сформировать промпт
//	  → вызвать Ollama API (Gemma)
//	  → обновить PostgreSQL (analysis_*, status='analyzed')
//	  → отправить в Kafka 'analysis-done'
//	  → опционально: POST вебхук в CRM
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

// ── промпт для анализа ──────────────────────────────────────────

const analysisPromptTpl = `Ты — специалист по анализу качества звонков в колл-центре.
Проанализируй транскрипцию телефонного разговора между сотрудником и клиентом.

ИНФОРМАЦИЯ О ЗВОНКЕ:
- Сотрудник: %s (ID: %s)
- Направление: %s
- Длительность: %d сек

ТРАНСКРИПЦИЯ:
%s

Ответь СТРОГО в формате JSON без markdown и лишнего текста:
{
  "summary": "краткое резюме звонка в 2-3 предложения",
  "sentiment": "positive или neutral или negative",
  "script_compliance": true,
  "script_violations": ["список нарушений, пустой массив если нарушений нет"],
  "action_items": ["список действий после звонка"],
  "score": 8,
  "topics": ["ключевые темы разговора"],
  "call_outcome": "sold или callback или rejected или escalated или consultation или other"
}`

// ── структуры событий ───────────────────────────────────────────

type TranscriptionDoneEvent struct {
	CallID       string `json:"call_id"`
	FilePath     string `json:"file_path"`
	Source       string `json:"source"`
	EmployeeID   string `json:"employee_id"`
	EmployeeName string `json:"employee_name"`
	PhoneFrom    string `json:"phone_from"`
	PhoneTo      string `json:"phone_to"`
	Direction    string `json:"direction"`
	Duration     int    `json:"duration"`
	UploadedAt   string `json:"uploaded_at"`
	Transcript   struct {
		FullText            string           `json:"full_text"`
		Language            string           `json:"language"`
		LanguageProbability float64          `json:"language_probability"`
		Segments            []map[string]any `json:"segments"`
	} `json:"transcript"`
}

type AnalysisResult struct {
	Summary          string   `json:"summary"`
	Sentiment        string   `json:"sentiment"`
	ScriptCompliance bool     `json:"script_compliance"`
	ScriptViolations []string `json:"script_violations"`
	ActionItems      []string `json:"action_items"`
	Score            int      `json:"score"`
	Topics           []string `json:"topics"`
	CallOutcome      string   `json:"call_outcome"`
}

// ── Ollama ──────────────────────────────────────────────────────

type ollamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
	Format string `json:"format"`
}

type ollamaResponse struct {
	Response string `json:"response"`
}

func analyzeWithOllama(ctx context.Context, client *http.Client, ollamaHost, model, prompt string) (*AnalysisResult, error) {
	reqBody, _ := json.Marshal(ollamaRequest{
		Model:  model,
		Prompt: prompt,
		Stream: false,
		Format: "json",
	})

	req, err := http.NewRequestWithContext(ctx, "POST", ollamaHost+"/api/generate", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ollama %d: %s", resp.StatusCode, body)
	}

	var ollamaResp ollamaResponse
	if err := json.Unmarshal(body, &ollamaResp); err != nil {
		return nil, fmt.Errorf("parse ollama response: %w", err)
	}

	var result AnalysisResult
	if err := json.Unmarshal([]byte(ollamaResp.Response), &result); err != nil {
		return nil, fmt.Errorf("parse analysis json: %w (raw: %s)", err, ollamaResp.Response)
	}
	return &result, nil
}

func pullModel(ctx context.Context, client *http.Client, ollamaHost, model string) {
	slog.Info("проверяю наличие модели", "model", model)

	// Пробуем quick check
	checkBody, _ := json.Marshal(map[string]any{
		"model": model, "prompt": "hi", "stream": false,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", ollamaHost+"/api/generate", bytes.NewReader(checkBody))
	req.Header.Set("Content-Type", "application/json")
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req = req.WithContext(checkCtx)
	if resp, err := client.Do(req); err == nil && resp.StatusCode == 200 {
		resp.Body.Close()
		slog.Info("модель готова", "model", model)
		return
	}

	slog.Info("скачиваю модель (может занять несколько минут)...", "model", model)
	pullBody, _ := json.Marshal(map[string]any{"name": model})
	pullReq, _ := http.NewRequestWithContext(ctx, "POST", ollamaHost+"/api/pull", bytes.NewReader(pullBody))
	pullReq.Header.Set("Content-Type", "application/json")

	pullClient := &http.Client{Timeout: 600 * time.Second}
	resp, err := pullClient.Do(pullReq)
	if err != nil {
		slog.Warn("pull model error", "err", err)
		return
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	for {
		var line map[string]any
		if err := dec.Decode(&line); err != nil {
			break
		}
		if status, ok := line["status"].(string); ok {
			if strings.ContainsAny(status, "pulling verifying success error") {
				slog.Info("ollama pull", "status", status)
			}
		}
	}
	slog.Info("модель готова", "model", model)
}

// ── CRM вебхук ──────────────────────────────────────────────────

func sendWebhook(ctx context.Context, client *http.Client, webhookURL string, payload map[string]any) {
	if webhookURL == "" {
		return
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", webhookURL, bytes.NewReader(body))
	if err != nil {
		slog.Warn("webhook create request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		slog.Warn("webhook send", "err", err)
		return
	}
	resp.Body.Close()
	slog.Info("вебхук отправлен", "url", webhookURL)
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

func jsonOrNull(v []string) []byte {
	if v == nil {
		v = []string{}
	}
	b, _ := json.Marshal(v)
	return b
}

// ── обработка одного события ────────────────────────────────────

func processRecord(
	ctx context.Context,
	record *kgo.Record,
	httpClient *http.Client,
	ollamaHost, ollamaModel string,
	crmWebhook string,
	kClient *kgo.Client,
	pool *pgxpool.Pool,
) error {
	var event TranscriptionDoneEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("parse event: %w", err)
	}

	callID := event.CallID
	if callID == "" {
		callID = "unknown"
	}
	slog.Info("анализирую", "call_id", callID)

	fullText := event.Transcript.FullText
	if strings.TrimSpace(fullText) == "" {
		slog.Warn("пустая транскрипция, пропускаю", "call_id", callID)
		return nil
	}

	empName := event.EmployeeName
	if empName == "" {
		empName = "неизвестен"
	}
	direction := event.Direction
	if direction == "" {
		direction = "не указано"
	}

	prompt := fmt.Sprintf(analysisPromptTpl,
		empName, event.EmployeeID,
		direction, event.Duration,
		fullText,
	)

	analysis, err := analyzeWithOllama(ctx, httpClient, ollamaHost, ollamaModel, prompt)
	if err != nil {
		return fmt.Errorf("ollama: %w", err)
	}

	// Обновляем PostgreSQL
	_, dbErr := pool.Exec(ctx,
		`UPDATE calls SET
		     status = 'analyzed',
		     analysis_summary = $1,
		     analysis_sentiment = $2,
		     analysis_script_compliance = $3,
		     analysis_script_violations = $4,
		     analysis_action_items = $5,
		     analysis_score = $6,
		     analysis_topics = $7,
		     analysis_call_outcome = $8,
		     analyzed_at = NOW()
		 WHERE call_id = $9`,
		analysis.Summary,
		analysis.Sentiment,
		analysis.ScriptCompliance,
		jsonOrNull(analysis.ScriptViolations),
		jsonOrNull(analysis.ActionItems),
		analysis.Score,
		jsonOrNull(analysis.Topics),
		analysis.CallOutcome,
		callID,
	)
	if dbErr != nil {
		slog.Warn("db update analysis", "call_id", callID, "err", dbErr)
	} else {
		logDB(ctx, pool, callID, "info", "analysis",
			fmt.Sprintf("Анализ завершён: score=%d, sentiment=%s", analysis.Score, analysis.Sentiment))
	}

	// Kafka: analysis-done
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
		"transcript":    event.Transcript,
		"analysis":      analysis,
		"analyzed_at":   time.Now().UTC().Format(time.RFC3339),
	}
	outJSON, _ := json.Marshal(outEvent)
	if err := kClient.ProduceSync(ctx, &kgo.Record{
		Topic: "analysis-done",
		Value: outJSON,
	}).FirstErr(); err != nil {
		slog.Warn("kafka produce analysis-done", "call_id", callID, "err", err)
	}

	// CRM вебхук
	sendWebhook(ctx, &http.Client{Timeout: 10 * time.Second}, crmWebhook, map[string]any{
		"call_id":       callID,
		"employee_id":   event.EmployeeID,
		"employee_name": event.EmployeeName,
		"phone_from":    event.PhoneFrom,
		"phone_to":      event.PhoneTo,
		"duration":      event.Duration,
		"analysis":      analysis,
	})

	slog.Info("готово", "call_id", callID,
		"score", analysis.Score,
		"sentiment", analysis.Sentiment,
		"outcome", analysis.CallOutcome,
	)
	return nil
}

// ── main ────────────────────────────────────────────────────────

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	kafkaBootstrap := getenv("KAFKA_BOOTSTRAP", "kafka:9092")
	postgresDSN := mustenv("POSTGRES_DSN")
	ollamaHost := getenv("OLLAMA_HOST", "http://ollama:11434")
	ollamaModel := getenv("OLLAMA_MODEL", "gemma3:4b")
	crmWebhook := getenv("CRM_WEBHOOK_URL", "")

	ctx := context.Background()

	// PostgreSQL
	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		slog.Error("postgres connect", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	httpClient := &http.Client{Timeout: 180 * time.Second}

	// Ждём Ollama и скачиваем модель
	for i := range 20 {
		resp, err := httpClient.Get(ollamaHost + "/api/tags")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			slog.Info("ollama готова")
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		slog.Warn("ollama ожидание", "attempt", i+1)
		time.Sleep(5 * time.Second)
	}
	pullModel(ctx, httpClient, ollamaHost, ollamaModel)

	// Kafka consumer + producer
	kClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaBootstrap, ",")...),
		kgo.ConsumerGroup("analyzer-group"),
		kgo.ConsumeTopics("transcriptions-done"),
		kgo.DisableAutoCommit(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.SessionTimeout(60*time.Second),
		kgo.HeartbeatInterval(20*time.Second),
	)
	if err != nil {
		slog.Error("kafka connect", "err", err)
		os.Exit(1)
	}
	defer kClient.Close()

	for i := range 20 {
		if err := kClient.Ping(ctx); err == nil {
			slog.Info("kafka готова")
			break
		}
		slog.Warn("kafka ожидание", "attempt", i+1)
		time.Sleep(3 * time.Second)
	}

	slog.Info("слушаю топик 'transcriptions-done'...")

	for {
		fetches := kClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				slog.Error("kafka fetch error", "err", e.Err)
			}
			continue
		}

		fetches.EachRecord(func(record *kgo.Record) {
			err := processRecord(ctx, record, httpClient, ollamaHost, ollamaModel, crmWebhook, kClient, pool)
			if err != nil {
				var callID string
				var ev TranscriptionDoneEvent
				if jsonErr := json.Unmarshal(record.Value, &ev); jsonErr == nil {
					callID = ev.CallID
				}
				slog.Error("ОШИБКА обработки", "call_id", callID, "err", err)

				if callID != "" {
					pool.Exec(ctx,
						`UPDATE calls SET status='failed_analysis' WHERE call_id=$1`,
						callID,
					)
					logDB(ctx, pool, callID, "error", "analysis", "Ошибка: "+err.Error())

					errEvent, _ := json.Marshal(map[string]any{
						"call_id": callID,
						"error":   err.Error(),
						"stage":   "analysis",
					})
					kClient.ProduceSync(ctx, &kgo.Record{
						Topic: "calls-errors",
						Value: errEvent,
					})
				}
			}

			kClient.CommitRecords(ctx, record)
		})
	}
}
