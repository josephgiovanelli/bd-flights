package org.queue.bd.airportsjob;

import pojos.Flight;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;


public class Test {
    public static void main(String[] args) throws Exception {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("/Users/josephgiovanelli/Universita/magistrale/2anno/2semestre/Big Data/progetto/flight-delays-senza-headers/flights.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                Flight flight = new Flight(line);
                if (flight.getOrigin_airport().length() > 3) {
                    System.out.println(flight.getOrigin_airport());
                }
            }
        }
    }
}
