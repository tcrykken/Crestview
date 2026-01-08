package models

import (
	"time"
)

type ReservationRaw struct {
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

type Reservation struct {
	LstGrp		string			`json:"lst_grp"`
	ConfCode	string			`json:"Confirmation_code"`
	AdultN		int				`json:"__of_adults"`
	ChildN		int				`json:"__of_children"`
	InfantN		int				`json:"__of_infants"`
	StartDt		time.Time		`json:"Start_date"`
	EndDt		time.Time		`json:"End_date"`
	Nights		int				`json:"__of_nights"`
	BookedDt	time.Time		`json:"Booked"`
	Earnings	float64			`json:"Earnings"`
}

type TransactionRaw struct {
	Amount			float64			`json:"amount"`
	GrossEarning	float64			`json:"gross_earning"`
	HostFee			float64			`json:"host_fee"`
	OccupancyTaxes	float64			`json:"occupancy_taxes"`
	CleaningFee		float64			`json:"cleaning_fee"`
	LstGrp			string			`json:"lst_grp"`
	ConfCode		string			`json:"Confirmation_code"`
	Currency		string			`json:"Currency"`
	Details			string			`json:"Details"`
	Nights			int				`json:"Nights"`
	PaidOut			float64			`json:"Paid_Out"`
	Reference		string			`json:"Reference"`
	StartDt			string		`json:"Start_Date"`
	Type			string			`json:"Type"`
	GrossEarnings   float64		 	`json:"gross_earnings"`
	Date 			string		`json:"Date"`
	Guest 			string			`json:"guest"`
}

type Transaction struct {
	Amount			float64			`json:"amount"`
	GrossEarning	float64			`json:"gross_earning"`
	HostFee			float64			`json:"host_fee"`
	OccupancyTaxes	float64			`json:"occupancy_taxes"`
	CleaningFee		float64			`json:"cleaning_fee"`
	LstGrp			string			`json:"lst_grp"`
	ConfCode		string			`json:"Confirmation_code"`
	Currency		string			`json:"Currency"`
	Details			string			`json:"Details"`
	Nights			int				`json:"Nights"`
	PaidOut			float64			`json:"Paid_Out"`
	Reference		string			`json:"Reference"`
	StartDt			time.Time		`json:"Start_Date"`
	Type			string			`json:"Type"`
	GrossEarnings   float64		 	`json:"gross_earnings"`
	Date 			time.Time		`json:"Date"`
	Guest 			string			`json:"guest"`
}