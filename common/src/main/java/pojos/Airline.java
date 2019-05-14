package pojos;

public class Airline {

    private final String iata_code;
    private final String airline;

    public Airline(final String line) {
        final String[] fields = line.replace(",", ", ").split(",");
        this.iata_code = fields[0].trim();
        this.airline = fields[1].trim();
    }

    public String getIata_code() {
        return iata_code;
    }

    public String getAirline() {
        return airline;
    }
}

