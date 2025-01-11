package com.example;

public class FlightSegment {
    private String origin;
    private String destination;
    private String departureDate;
    private double price;

    // Constructor
    public FlightSegment(String origin, String destination, String departureDate, double price) {
        this.origin = origin;
        this.destination = destination;
        this.departureDate = departureDate;
        this.price = price;
    }

    // Getter for origin
    public String getOrigin() {
        return origin;
    }

    // Getter for destination
    public String getDestination() {
        return destination;
    }

    // Getter for departureDate
    public String getDepartureDate() {
        return departureDate;
    }

    // Getter for price
    public double getPrice() {
        return price;
    }
}
