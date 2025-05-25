package main

import (
	"database/sql"
	"log"
	"os"

	pricehistoryimporter "db-importer/price-history"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

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

	importer := pricehistoryimporter.NewPriceHistoryImporter(db)

	if err := importer.InitializeTable(); err != nil {
		log.Fatal("Failed to create table:", err)
	}

	csvLocation := os.Getenv("CSV_LOCATION")
	if err := importer.ImportFromCSV(csvLocation); err != nil {
		log.Fatal("Failed to import CSV:", err)
	}

	log.Println("CSV import completed successfully")
}
