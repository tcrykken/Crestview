package models

import (
	"time"
)

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