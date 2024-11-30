package models

// SearchRoom represents the details of a single room in a hotel search.
type SearchRoom struct {
	Room      int    `json:"Room"`       // Number of rooms to search for.
	Adult     int    `json:"adult"`      // Number of adults staying in the room.
	Child     int    `json:"child"`      // Number of children staying in the room.
	TrackerID string `json:"tracker_id"` // Identifier to track the search request.
	ChildAge  []int  `json:"childAge"`   // Ages of the children staying in the room.
}

// HotelSearchRequest represents the parameters for a hotel search query.
type HotelSearchRequest struct {
	Country          string       `json:"country"`          // Country where the hotel is located.
	CheckinDate      string       `json:"checkinDate"`      // Check-in date for the booking (format: YYYY-MM-DD).
	CheckoutDate     string       `json:"checkoutDate"`     // Check-out date for the booking (format: YYYY-MM-DD).
	SearchRooms      []SearchRoom `json:"searchRooms"`      // List of room details included in the search.
	HotelCode        string       `json:"hotelcode"`        // Unique code of the specific hotel being searched.
	Nationality      string       `json:"nationality"`      // Nationality of the guest(s) for specific pricing or rules.
	MealOptions      string       `json:"mealOptions"`      // Meal preferences for the booking (e.g., breakfast included).
	TrackerID        string       `json:"tracker_id"`       // Identifier to track the search request.
	ChepeastRoomOnly bool         `json:"chepeastRoomOnly"` // If true, only search for the cheapest room available.
}
