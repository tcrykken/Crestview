package models



type Reservation struct {
	LstGrp				string		`json:"lst_grp"`
	ConfCode			string		`"json:"Confirmation_code"`
	AdultN				int			`"json:"__of_adults"`
	ChildN				int			`"json:"__of_children"`
	InfantN				int			`"json:"__of_infants"`
}