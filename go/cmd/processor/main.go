package main

import (
	"fmt"

	"github.com/tcrykken/Crestview/internal/models"
	"github.com/tcrykken/Crestview/internal/reader"
)

type reservations []models.Reservation
type transactions []models.Transaction

func main() {
	reservationsData, err := reader.ReadResJSON("/Users/a2338-home/Documents/Crestview/305Analysis/data/input/reservations_02NOV25.json")
	if err != nil {
		fmt.Printf("error reading JSON file: %v", err)
		return
	}
	var res reservations = reservations(reservationsData)

	fmt.Println("reservations[1]:", res[1])
	fmt.Println("reservations[1].LstGrp:", res[1].LstGrp)
	fmt.Println("reservations[1].ConfCode:", res[1].ConfCode)
	fmt.Println("reservations[1].AdultN:", res[1].AdultN)
	fmt.Println("reservations[1].ChildN:", res[1].ChildN)
	fmt.Println("reservations[1].InfantN:", res[1].InfantN)
	fmt.Println("reservations[1].StartDt:", res[1].StartDt)
	fmt.Println("reservations[1].EndDt:", res[1].EndDt)
	fmt.Println("reservations[1].Nights:", res[1].Nights)
	fmt.Println("reservations[1].BookedDt:", res[1].BookedDt)
	fmt.Println("reservations[1].Earnings:", res[1].Earnings)

	transactionsData, err := reader.ReadTxJson("/Users/a2338-home/Documents/Crestview/305Analysis/data/input/transactions_02NOV25.json")
	if err != nil {
		fmt.Printf("error reading JSON file: %v", err)
		return
	}
	var txs transactions = transactions(transactionsData)

	fmt.Println("transactions[1]:", txs[1])
	fmt.Println("transactions[1].Amount:", txs[1].Amount)
	fmt.Println("transactions[1].GrossEarning:", txs[1].GrossEarning)
	fmt.Println("transactions[1].HostFee:", txs[1].HostFee)
	fmt.Println("transactions[1].OccupancyTaxes:", txs[1].OccupancyTaxes)
	fmt.Println("transactions[1].CleaningFee:", txs[1].CleaningFee)
	fmt.Println("transactions[1].LstGrp:", txs[1].LstGrp)
	fmt.Println("transactions[1].ConfCode:", txs[1].ConfCode)
	fmt.Println("transactions[1].Currency:", txs[1].Currency)
	fmt.Println("transactions[1].Details:", txs[1].Details)
	fmt.Println("transactions[1].Nights:", txs[1].Nights)
	fmt.Println("transactions[1].PaidOut:", txs[1].PaidOut)
	fmt.Println("transactions[1].Reference:", txs[1].Reference)
	fmt.Println("transactions[1].StartDt:", txs[1].StartDt)
	fmt.Println("transactions[1].Type:", txs[1].Type)
	fmt.Println("transactions[1].GrossEarnings:", txs[1].GrossEarnings)
	fmt.Println("transactions[1].Date:", txs[1].Date)
	fmt.Println("transactions[1].Guest:", txs[1].Guest)
}
