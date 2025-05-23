package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type AssetPrice struct {
	Id      string
	Date    time.Time
	AssetID string
	Close   float64
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
		CREATE TABLE IF NOT EXISTS PriceHistory (
			Id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
			Date DATE NOT NULL,
			AssetId VARCHAR(50) NOT NULL,
			Price NUMERIC NOT NULL,
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

	stmt, err := db.Prepare("INSERT INTO PriceHistory (Date, AssetId, Price) VALUES ($1, $2, $3)")
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

		date, err := time.Parse("1/2/06", record[0])
		if err != nil {
			log.Printf("Error parsing date '%s': %v", record[0], err)
			continue
		}

		var price float64
		_, err = fmt.Sscanf(record[2], "%f", &price)
		if err != nil {
			log.Printf("Error parsing price '%s': %v", record[2], err)
			continue
		}

		formattedDate := date.Format("02/01/2006")
		_, err = stmt.Exec(formattedDate, record[1], price)
		if err != nil {
			log.Printf("Error inserting record: %v", err)
			continue
		}
	}

	fmt.Println("CSV import completed successfully")
}
