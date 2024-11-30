package models

// AvailabilityRequest represents the structure for a hotel availability search request.
type AvailabilityRequest struct {
	Stay         Stay            `json:"stay"`             // Details of the stay, including check-in and check-out dates.
	Occupancies  []Occupancy     `json:"occupancies"`      // List of occupancies specifying room and guest details.
	Hotels       HotelBedsHotels `json:"hotels"`           // List of specific hotels to search.
	Filter       Filter          `json:"filter"`           // Search filters such as max rooms, payment type, etc.
	DailyRate    bool            `json:"dailyRate"`        // Flag to include daily rate breakdown in the response.
	Platform     string          `json:"platform"`         // Platform identifier for the request source.
	SourceMarket string          `json:"sourceMarket"`     // Market source for pricing and availability.
	Boards       *Boards         `json:"boards,omitempty"` // Board options (e.g., meal plans), optional.
}

// Boards represents meal plan options for the search request.
type Boards struct {
	Board    []string `json:"board,omitempty"`    // List of board types (e.g., breakfast, all-inclusive).
	Included bool     `json:"included,omitempty"` // Indicates if the boards are included in the search.
}

// Destination represents the destination details in a request.
type Destination struct {
	Code string `json:"Code"` // Code identifying the destination (e.g., city).
}

// Filter defines filtering options for the availability request.
type Filter struct {
	MaxRooms        int    `json:"maxRooms,omitempty"`        // Maximum number of rooms to return in the response.
	MaxRatesPerRoom int    `json:"maxRatesPerRoom,omitempty"` // Maximum number of rates per room in the response.
	PaymentType     string `json:"paymentType"`               // Preferred payment type (e.g., "prepaid" or "postpaid").
}

// HotelBedsHotels represents a collection of specific hotels in the request.
type HotelBedsHotels struct {
	Hotel []int `json:"hotel"` // List of hotel IDs to include in the search.
}

// Occupancy represents room and guest details for the search request.
type Occupancy struct {
	Rooms    int   `json:"rooms"`    // Number of rooms in the request.
	Adults   int   `json:"adults"`   // Number of adults in the occupancy.
	Children int   `json:"children"` // Number of children in the occupancy.
	Paxes    []Pax `json:"paxes"`    // Detailed list of all guests, including age and type.
}

// Stay contains the check-in and check-out dates for the booking.
type Stay struct {
	CheckinDate  string `json:"checkIn"`  // Check-in date in the format YYYY-MM-DD.
	CheckoutDate string `json:"checkOut"` // Check-out date in the format YYYY-MM-DD.
}

// Pax represents an individual guest in the occupancy.
type Pax struct {
	Type string `json:"type"`          // Type of guest (e.g., "adult" or "child").
	Age  int    `json:"age,omitempty"` // Age of the guest, optional for adults.
}

// CommonResp represents a common response structure for supplier and room details.
type CommonResp struct {
	SupplierRequest string `json:"supplier_request"` // Request details sent to the supplier.
	RoomInfo        string `json:"room_info"`        // Information about the room(s) in the response.
}
