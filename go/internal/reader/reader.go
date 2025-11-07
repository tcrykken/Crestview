package reader

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tcrykken/Crestview/internal/models"
)

const dateLayout = "2006-01-02"

// Helper function to parse the date or return zero time
func parseDateOrZero(dateStr string) (dt time.Time, err error) {
	if dateStr == "" {
		return time.Time{}, nil
	}
	return time.Parse(dateLayout, dateStr)
}

func ReadJSON(filepath string) ([]models.Reservation, error) {
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

	type reservationRaw struct {
		LstGrp   string  `json:"lst_grp"`
		ConfCode string  `json:"Confirmation_code"`
		AdultN   int     `json:"__of_adults"`
		ChildN   int     `json:"__of_children"`
		InfantN  int     `json:"__of_infants"`
		StartDt  string  `json:"Start_date"`
		EndDt    string  `json:"End_date"`
		Nights   int     `json:"__of_nights"`
		BookedDt string  `json:"Booked"`
		Earnings float64 `json:"Earnings"`
	}

	var reservationsRaw []reservationRaw
	err = json.Unmarshal(byt, &reservationsRaw)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling file: %v", err)
	}

	var reservations []models.Reservation
	for _, r := range reservationsRaw {
		startDt, err := parseDateOrZero(r.StartDt)
		if err != nil {
			return nil, fmt.Errorf("invalid StartDt: %v", err)
		}
		endDt, err := parseDateOrZero(r.EndDt)
		if err != nil {
			return nil, fmt.Errorf("invalid EndDt: %v", err)
		}
		bookedDt, err := parseDateOrZero(r.BookedDt)
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
