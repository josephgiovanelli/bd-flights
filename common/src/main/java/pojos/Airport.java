package pojos;

/**
 * Pojo class to store an airport information
 */
public class Airport {

    private final String iata_code;
    private final String airport;

    public Airport(final String line) {
        final String[] fields = line.replace(",", ", ").split(",");
        this.iata_code = fields[0].trim();
        this.airport = fields[1].trim();
    }

    public String getIata_code() {
        return iata_code;
    }

    public String getAirport() {
        return airport;
    }
}
