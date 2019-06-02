package pojos;

/**
 * Pojo class to store an airport information
 */
public class Airport {

    private final String iata_code;
    private final String airport;
    private final String city;
    private final String state;
    private final String country;
    private final String latitude;
    private final String longitude;

    public Airport(final String line) {
        final String[] fields = line.replace(",", ", ").split(",");
        this.iata_code = fields[0].trim();
        this.airport = fields[1].trim();
        this.city = fields[2].trim();
        this.state = fields[3].trim();
        this.country = fields[4].trim();
        this.latitude = fields[5].trim();
        this.longitude = fields[6].trim();
    }

    public String getIata_code() {
        return iata_code;
    }

    public String getAirport() {
        return airport;
    }

    public String getCity() {
        return city;
    }

    public String getState() {
        return state;
    }

    public String getCountry() {
        return country;
    }

    public String getLatitude() {
        return latitude;
    }

    public String getLongitude() {
        return longitude;
    }
}
