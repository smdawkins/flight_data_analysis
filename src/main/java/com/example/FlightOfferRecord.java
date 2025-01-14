package com.example;
import java.time.LocalDateTime;

public class FlightOfferRecord {
    private String id;
    private String lastTicketingDate;
    private int numberOfBookableSeats;
    private String duration;
    private String totalPrice;
    private String currency;
    private String validatingAirlineCodes;
    private int flightSegments;
    private String departureTime;
    private String arrivalTime;
	private LocalDateTime insertTime;
	private String departureIataCode;
	private String arrivalIataCode;


    // Constructor
    public FlightOfferRecord(String id, String lastTicketingDate, int numberOfBookableSeats, String duration,
                         String totalPrice, String currency, String validatingAirlineCodes, int flightSegments,
                         String departureTime, String arrivalTime, String departureIataCode, String arrivalIataCode) {
		this.id = id;
		this.lastTicketingDate = lastTicketingDate;
		this.numberOfBookableSeats = numberOfBookableSeats;
		this.duration = duration;
		this.totalPrice = totalPrice;
		this.currency = currency;
		this.validatingAirlineCodes = validatingAirlineCodes;
		this.flightSegments = flightSegments;
		this.departureTime = departureTime;
		this.arrivalTime = arrivalTime;
		this.insertTime = LocalDateTime.now();
		this.departureIataCode = departureIataCode;
		this.arrivalIataCode = arrivalIataCode;
	}

    // Getters and setters (only for insertTime, if others already exist)
    public LocalDateTime getInsertTime() {
        return insertTime;
    }

    public void setInsertTime(LocalDateTime insertTime) {
        this.insertTime = insertTime;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLastTicketingDate() {
        return lastTicketingDate;
    }

    public void setLastTicketingDate(String lastTicketingDate) {
        this.lastTicketingDate = lastTicketingDate;
    }

    public int getNumberOfBookableSeats() {
        return numberOfBookableSeats;
    }

    public void setNumberOfBookableSeats(int numberOfBookableSeats) {
        this.numberOfBookableSeats = numberOfBookableSeats;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(String totalPrice) {
        this.totalPrice = totalPrice;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getValidatingAirlineCodes() {
        return validatingAirlineCodes;
    }

    public void setValidatingAirlineCodes(String validatingAirlineCodes) {
        this.validatingAirlineCodes = validatingAirlineCodes;
    }

    public int getFlightSegments() {
        return flightSegments;
    }

    public void setFlightSegments(int flightSegments) {
        this.flightSegments = flightSegments;
    }

    public String getDepartureTime() {
        return departureTime;
    }

    public void setDepartureTime(String departureTime) {
        this.departureTime = departureTime;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(String arrivalTime) {
        this.arrivalTime = arrivalTime;
    }
	public String getDepartureIataCode() {
		return departureIataCode;
	}

	public void setDepartureIataCode(String departureIataCode) {
		this.departureIataCode = departureIataCode;
	}

	public String getArrivalIataCode() {
		return arrivalIataCode;
	}

	public void setArrivalIataCode(String arrivalIataCode) {
		this.arrivalIataCode = arrivalIataCode;
	}

}
