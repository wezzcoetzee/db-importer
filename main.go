package main

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
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type AssetPrice struct {
	Id      string
	Date    time.Time
	AssetID string
	Close   float64
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

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	connStr := os.Getenv("DB_CONNECTION_STRING")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	createTableQuery := `
		CREATE TABLE IF NOT EXISTS "PriceHistory" (
			"Id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			"Date" DATE NOT NULL,
			"AssetId" VARCHAR(50) NOT NULL,
			"Price" NUMERIC NOT NULL
		)`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	location := os.Getenv("CSV_LOCATION")
	file, err := os.Open(location)
	if err != nil {
		log.Fatal("Failed to open CSV file:", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 3

	_, err = reader.Read()
	if err != nil {
		log.Fatal("Failed to read CSV header:", err)
	}

	stmt, err := db.Prepare(`INSERT INTO "PriceHistory" ("Id", "Date", "AssetId", "Price") VALUES ($1, $2, $3, $4)`)
	if err != nil {
		log.Fatal("Failed to prepare SQL statement:", err)
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

		log.Printf("Successfully inserted record: Id=%s, Date=%s, AssetId=%s, pPricerice=%.2f, rows_affected=%d", id, date, record[1], price, rowsAffected)
	}

	fmt.Println("CSV import completed successfully")
}
