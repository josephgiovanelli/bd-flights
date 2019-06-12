package pojos;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * For debug purposes
 */
public class Test {
    public static void main(String[] args) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader("/Users/josephgiovanelli/Universita/magistrale/2anno/2semestre/Big Data/progetto/flight-delays-senza-headers/airlines.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                Airline airline = new Airline(line);
                System.out.println(airline.getIata_code() + " " + airline.getAirline());
            }
        }
    }
}
