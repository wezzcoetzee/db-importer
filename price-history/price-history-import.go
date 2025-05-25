package pricehistoryimporter

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

type AssetPrice struct {
	Id      string
	Date    time.Time
	AssetID string
	Close   float64
}

type PriceHistoryImporter struct {
	db *sql.DB
}

func NewPriceHistoryImporter(db *sql.DB) *PriceHistoryImporter {
	return &PriceHistoryImporter{db: db}
}

func (i *PriceHistoryImporter) InitializeTable() error {
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS "PriceHistory" (
			"Id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			"Date" DATE NOT NULL,
			"AssetId" VARCHAR(50) NOT NULL,
			"Price" NUMERIC NOT NULL
		)`
	_, err := i.db.Exec(createTableQuery)
	return err
}

func normalizeDate(dateStr string) string {
	parts := strings.Split(dateStr, "/")
	if len(parts) != 3 {
		return dateStr
	}

	day := parts[0]
	if len(day) == 1 {
		day = "0" + day
	}
	month := parts[1]
	if len(month) == 1 {
		month = "0" + month
	}
	year := parts[2]

	return fmt.Sprintf("%s/%s/%s", day, month, year)
}

func (i *PriceHistoryImporter) ImportFromCSV(csvLocation string) error {
	file, err := os.Open(csvLocation)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 3

	_, err = reader.Read() // Skip header
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	stmt, err := i.db.Prepare(`INSERT INTO "PriceHistory" ("Id", "Date", "AssetId", "Price") VALUES ($1, $2, $3, $4)`)
	if err != nil {
		return fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer stmt.Close()

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV record: %v", err)
			continue
		}

		normalizedDate := normalizeDate(record[0])
		date, err := time.ParseInLocation("02/01/2006", normalizedDate, time.UTC)
		if err != nil {
			log.Printf("Error parsing date '%s': %v", normalizedDate, err)
			continue
		}

		var price float64
		_, err = fmt.Sscanf(record[2], "%f", &price)
		if err != nil {
			log.Printf("Error parsing price '%s': %v", record[2], err)
			continue
		}

		id := uuid.New().String()

		result, err := stmt.Exec(id, date, record[1], price)
		if err != nil {
			log.Printf("Error inserting record (Id: %s, Date: %s, AssetId: %s, Price: %.2f): %v", id, date, record[1], price, err)
			continue
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			log.Printf("Error checking rows affected for record (id: %s): %v", id, err)
			continue
		}

		log.Printf("Successfully inserted record: Id=%s, Date=%s, AssetId=%s, Price=%.2f, rows_affected=%d", id, date, record[1], price, rowsAffected)
	}

	return nil
}
