package main

import (
	"fmt"

	"github.com/tcrykken/Crestview/internal/models"
	"github.com/tcrykken/Crestview/internal/reader"
)

type reservations []models.Reservation

func main() {
	reservationsData, err := reader.ReadJSON("/Users/a2338-home/Documents/Crestview/305Analysis/data/input/reservations_02NOV25.json")
	if err != nil {
		fmt.Printf("error reading JSON file: %v", err)
		return
	}
	var res reservations = reservations(reservationsData)

	fmt.Println(res[0])
	fmt.Println(res[1].LstGrp)
	fmt.Println(res[1].ConfCode)
	fmt.Println(res[1].AdultN)
	fmt.Println(res[1].ChildN)
	fmt.Println(res[1].InfantN)
	fmt.Println(res[1].StartDt)
	fmt.Println(res[1].EndDt)
	fmt.Println(res[1].Nights)
	fmt.Println(res[1].BookedDt)
	fmt.Println(res[1].Earnings)
}

