package services

import (
	"Search/constant"
	model "Search/models"

	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type SearchRepository interface {
	SearchRequestMapper(searchRQ model.HotelSearchRequest) string
}

type searchRepository struct {
}

func NewSearchRequestRepository() SearchRepository {
	return &searchRepository{}
}

// SearchRequestMapper maps the search request into an AvailabilityRequest and a CommonResp object.
// It processes the HotelSearchRequest and prepares the necessary structures for sending an availability request.
// The function returns the serialized JSON response.
func (s *searchRepository) SearchRequestMapper(searchRQ model.HotelSearchRequest) string {
	// Initialize the AvailabilityRequest and CommonResp objects
	var rq = model.AvailabilityRequest{}
	var request = model.CommonResp{}

	// Map Stay (CheckinDate and CheckoutDate) from the search request to AvailabilityRequest
	rq.Stay = model.Stay{
		CheckinDate:  ConvertStringToDate(searchRQ.CheckinDate),  // Convert the CheckinDate from string to the appropriate format
		CheckoutDate: ConvertStringToDate(searchRQ.CheckoutDate), // Convert the CheckoutDate from string to the appropriate format
	}

	// Parse the HotelCode from the search request and convert them to integer IDs
	var hotelIds []int
	for _, str := range strings.Split(searchRQ.HotelCode, ",") {
		// Convert each hotel code (string) into an integer and append to the hotelIds list
		if id, err := strconv.Atoi(str); err == nil {
			hotelIds = append(hotelIds, id)
		}
	}
	// Assign the list of hotel IDs to the AvailabilityRequest
	rq.Hotels = model.HotelBedsHotels{Hotel: hotelIds}

	// Set source market and platform for the request
	rq.SourceMarket = searchRQ.Country // Set the source market to the provided country
	rq.Platform = constant.PlatformTag // Set the platform tag (constant value)

	// Initialize the filter structure and set conditions for room and rate filtering
	var filter = model.Filter{}
	if searchRQ.ChepeastRoomOnly {
		// If the cheapest room only option is true, limit to one rate per room and set max rooms to the number of rooms in the search
		filter.MaxRooms = len(searchRQ.SearchRooms)
		filter.MaxRatesPerRoom = 1
	}
	filter.PaymentType = "AT_WEB" // Set the payment type to "AT_WEB" (web payment)
	rq.Filter = filter            // Assign the filter to the AvailabilityRequest

	// Set the flag to include daily rate information in the response
	rq.DailyRate = true

	// If meal options are provided in the search request, add them to the request
	if searchRQ.MealOptions != "" {
		rq.Boards = MealsAdded(searchRQ.MealOptions) // Map meal options to the Boards struct
	}

	// Create the occupancy information for the rooms in the search request
	rq.Occupancies = CreateOccupancies(searchRQ.SearchRooms, searchRQ.TrackerID)

	// Marshal the AvailabilityRequest struct to JSON and set it as SupplierRequest in the CommonResp
	searchrq, _ := json.Marshal(rq)
	request.SupplierRequest = string(searchrq)

	// Marshal the original search rooms to JSON and set it as RoomInfo in the CommonResp
	rinfo, _ := json.Marshal(searchRQ.SearchRooms)
	request.RoomInfo = string(rinfo)

	// Marshal the entire CommonResp struct to JSON and return it as a string
	mainrequest, _ := json.Marshal(request)
	return string(mainrequest)
}

// ConvertStringToDate converts a date string in the format "YYYY-MM-DD" to a Go time object formatted back to a string.
// If the string cannot be parsed, it returns an empty string.
func ConvertStringToDate(dateString string) string {
	// Attempt to parse the input string into a Go time object
	parsedDate, err := time.Parse("2006-01-02", dateString)
	if err != nil {
		// If there is an error (invalid format), return an empty string
		return ""
	}
	// Successfully parsed date, return the formatted date as a string in "YYYY-MM-DD" format
	return parsedDate.Format("2006-01-02")
}

// MealsAdded maps the provided meal option string to the corresponding board types and returns a pointer to a Boards struct.
// It looks up the meal options in a predefined map and appends the matching board codes to the result.
func MealsAdded(meals string) *model.Boards {
	// Define a lookup table for meal options to corresponding board codes
	mealLookup := map[string][]string{
		"1": {"RO"},                                                             // Room Only
		"2": {"B1", "AB", "B2", "CB", "BB", "BF", "EB", "IB", "LB", "QB", "SB"}, // Bed and Breakfast options
		"3": {"HB", "HR", "FS", "HL", "HS", "HV"},                               // Half Board options
		"4": {"FB", "FE", "FL", "FR", "FS", "FV"},                               // Full Board options
		"5": {"AI", "AS", "AA"},                                                 // All-Inclusive options
	}

	// Split the provided meal options string by commas
	meal := strings.Split(meals, ",")
	var board []string

	// Iterate over each meal option and look up the corresponding board codes
	for _, item := range meal {
		// If the meal option is found in the lookup table, append the corresponding boards to the result
		if mealsList, found := mealLookup[item]; found {
			board = append(board, mealsList...)
		}
	}

	// Return a pointer to a Boards struct with the resulting board options and included flag set to true
	return &model.Boards{
		Board:    board,
		Included: true,
	}
}

// CreateOccupancies converts the searchHotelRequest (a slice of SearchRoom) to a list of occupancies.
// It assigns a unique TrackerID to each SearchRoom and generates corresponding Occupancy objects.
func CreateOccupancies(SearchRooms []model.SearchRoom, Tracker_Id string) []model.Occupancy {
	var occupancies []model.Occupancy

	// If no Tracker_Id is provided, generate a new unique Tracker_Id using UUID
	if Tracker_Id == "" {
		Tracker_Id = uuid.New().String()
	}

	// Iterate over the provided SearchRooms and convert each SearchRoom to an Occupancy
	roomNumber := 1
	for i, searchRoom := range SearchRooms {
		// Create an Occupancy object for the current room
		occupancies = append(occupancies, CreateOccupancy(searchRoom, roomNumber))

		// Increment the room number for the next iteration
		roomNumber++

		// Assign the provided or generated TrackerID to the current SearchRoom
		searchRoom.TrackerID = Tracker_Id

		// Update the SearchRooms slice with the modified SearchRoom
		SearchRooms[i] = searchRoom
	}

	// Return the list of Occupancy objects
	return occupancies
}

// CreateOccupancy creates a single occupancy object with Pax data for a given room and room count.
// The function takes a SearchRoom and the room count, and returns an Occupancy object with the relevant Pax information.
func CreateOccupancy(room model.SearchRoom, count int) model.Occupancy {
	// Initialize an Occupancy object with adults, children, and empty Pax slice.
	occupancy := model.Occupancy{
		Adults:   room.Adult,
		Children: room.Child,
		Paxes:    []model.Pax{}, // Start with an empty slice of Pax
		Rooms:    count,
	}

	// Add Pax entries for adults. Each adult gets an "AD" type.
	for i := 0; i < occupancy.Adults; i++ {
		occupancy.Paxes = append(occupancy.Paxes, model.Pax{Type: "AD"})
	}

	// Add Pax entries for children. Each child gets a "CH" type and age from room.ChildAge.
	for i := 0; i < occupancy.Children; i++ {
		occupancy.Paxes = append(occupancy.Paxes, model.Pax{
			Type: "CH",             // "CH" for child
			Age:  room.ChildAge[i], // Age for the child from the ChildAge array
		})
	}

	// Return the fully populated Occupancy object
	return occupancy
}
