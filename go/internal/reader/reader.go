package reader

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tcrykken/Crestview/internal/models"
)

const dashDateLayout = "2006-01-02"
const slashDateLayout = "01/02/2006"

// Helper function to parse the date or return zero time
func parseDashDateOrZero(dateStr string) (dt time.Time, err error) {
	if dateStr == "" {
		return time.Time{}, nil
	}
	return time.Parse(dashDateLayout, dateStr)
}

func parseSlashDateOrZero(dateStr string) (dt time.Time, err error) {
	if dateStr == "" {
		return time.Time{}, nil
	}
	return time.Parse(slashDateLayout, dateStr)
}

func ReadResJSON(filepath string) ([]models.Reservation, error) {
	jsonFile, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer jsonFile.Close()

	byt, err := io.ReadAll(jsonFile)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}
	// To parse date strings to time.Time for the Reservation struct during JSON unmarshalling, follow these steps:
	// 1. Create an inline type with the same structure as models.Reservation but with StartDt/EndDt/BookedDt as strings.
	// 2. Unmarshal the JSON file into a slice of this temporary type.
	// 3. Iterate through the slice and convert date strings to time.Time using time.Parse.
	// 4. Construct a slice of models.Reservation with parsed date fields.

	var reservationRaw []models.ReservationRaw
	err = json.Unmarshal(byt, &reservationRaw)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling file: %v", err)
	}

	var reservations []models.Reservation
	for _, r := range reservationRaw {
		startDt, err := parseDashDateOrZero(r.StartDt)
		if err != nil {
			return nil, fmt.Errorf("invalid StartDt: %v", err)
		}
		endDt, err := parseDashDateOrZero(r.EndDt)
		if err != nil {
			return nil, fmt.Errorf("invalid EndDt: %v", err)
		}
		bookedDt, err := parseDashDateOrZero(r.BookedDt)
		if err != nil {
			return nil, fmt.Errorf("invalid BookedDt: %v", err)
		}

		reservations = append(reservations, models.Reservation{
			LstGrp:   r.LstGrp,
			ConfCode: r.ConfCode,
			AdultN:   r.AdultN,
			ChildN:   r.ChildN,
			InfantN:  r.InfantN,
			StartDt:  startDt,
			EndDt:    endDt,
			Nights:   r.Nights,
			BookedDt: bookedDt,
			Earnings: r.Earnings,
		})
	}

	return reservations, nil
}

func ReadTxJson(filepath string) ([]models.Transaction, error) {
	jsonFile, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer jsonFile.Close()

	byt, err := io.ReadAll(jsonFile)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	var transactionsRaw []models.TransactionRaw
	err = json.Unmarshal(byt, &transactionsRaw)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling file: %v", err)
	}

	var transactions []models.Transaction
	for _, t := range transactionsRaw {
		startDt, err := parseSlashDateOrZero(t.StartDt)
		if err != nil {
			return nil, fmt.Errorf("invalid StartDt: %v", err)
		}
		date, err := parseSlashDateOrZero(t.Date)
		if err != nil {
			return nil, fmt.Errorf("invalid Date: %v", err)
		}

		transactions = append(transactions, models.Transaction{
			Amount: t.Amount,
			GrossEarning: t.GrossEarning,
			HostFee: t.HostFee,
			OccupancyTaxes: t.OccupancyTaxes,
			CleaningFee: t.CleaningFee,
			LstGrp: t.LstGrp,
			ConfCode: t.ConfCode,
			Currency: t.Currency,
			Details: t.Details,
			Nights: t.Nights,
			PaidOut: t.PaidOut,
			Reference: t.Reference,
			StartDt: startDt,
			Type: t.Type,
			GrossEarnings: t.GrossEarnings,
			Date: date,
			Guest: t.Guest,
		})
	}

	return transactions, nil
}