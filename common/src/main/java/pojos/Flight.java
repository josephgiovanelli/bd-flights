package pojos;

/**
 * Pojo class to store information related to a give flight
 */
public class Flight {


    private final String airline;
    private final String origin_airport;
    private final String scheduled_departure;
    private final String taxi_out;
    private final String arrival_delay;

    public Flight(final String line) {
        final String[] fields = line.replace(",", ", ").split(",");
        this.airline = fields[0].trim();
        this.origin_airport = fields[1].trim();
        this.scheduled_departure = fields[2].trim();
        this.taxi_out = fields[3].trim();
        this.arrival_delay = fields[4].trim();
    }

    public String getAirline() {
        return airline;
    }

    public String getOrigin_airport() {
        return origin_airport;
    }

    public String getScheduled_departure() {
        return scheduled_departure;
    }

    public String getTaxi_out() {
        return taxi_out;
    }

    public String getArrival_delay() {
        return arrival_delay;
    }
}
