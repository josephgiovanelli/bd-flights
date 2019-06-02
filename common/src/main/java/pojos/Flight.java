package pojos;

/**
 * Pojo class to store information related to a give flight
 */
public class Flight {

    private final String year;
    private final String month;
    private final String day;
    private final String day_of_week;
    private final String airline;
    private final String flight_number;
    private final String tail_number;
    private final String origin_airport;
    private final String destination_airport;
    private final String scheduled_departure;
    private final String departure_time;
    private final String departure_delay;
    private final String taxi_out;
    private final String wheels_off;
    private final String scheduled_time;
    private final String elapsed_time;
    private final String air_time;
    private final String distance;
    private final String wheels_on;
    private final String taxi_in;
    private final String scheduled_arrival;
    private final String arrival_time;
    private final String arrival_delay;
    private final String diverted;
    private final String cancelled;
    private final String cancellation_reason;
    private final String air_system_delay;
    private final String security_delay;
    private final String airline_delay;
    private final String late_aircraft_delay;
    private final String weather_delay;

    public Flight(final String line) {
        final String[] fields = line.replace(",", ", ").split(",");
        this.year = fields[0].trim();
        this.month = fields[1].trim();
        this.day = fields[2].trim();
        this.day_of_week = fields[3].trim();
        this.airline = fields[4].trim();
        this.flight_number = fields[5].trim();
        this.tail_number = fields[6].trim();
        this.origin_airport = fields[7].trim();
        this.destination_airport = fields[8].trim();
        this.scheduled_departure = fields[9].trim();
        this.departure_time = fields[10].trim();
        this.departure_delay = fields[11].trim();
        this.taxi_out = fields[12].trim();
        this.wheels_off = fields[13].trim();
        this.scheduled_time = fields[14].trim();
        this.elapsed_time = fields[15].trim();
        this.air_time = fields[16].trim();
        this.distance = fields[17].trim();
        this.wheels_on = fields[18].trim();
        this.taxi_in = fields[19].trim();
        this.scheduled_arrival = fields[20].trim();
        this.arrival_time = fields[21].trim();
        this.arrival_delay = fields[22].trim();
        this.diverted = fields[23].trim();
        this.cancelled = fields[24].trim();
        this.cancellation_reason = fields[25].trim();
        this.air_system_delay = fields[26].trim();
        this.security_delay = fields[27].trim();
        this.airline_delay = fields[28].trim();
        this.late_aircraft_delay = fields[29].trim();
        this.weather_delay = fields[30].trim();
    }

    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getDay() {
        return day;
    }

    public String getDay_of_week() {
        return day_of_week;
    }

    public String getAirline() {
        return airline;
    }

    public String getFlight_number() {
        return flight_number;
    }

    public String getTail_number() {
        return tail_number;
    }

    public String getOrigin_airport() {
        return origin_airport;
    }

    public String getDestination_airport() {
        return destination_airport;
    }

    public String getScheduled_departure() {
        return scheduled_departure;
    }

    public String getDeparture_time() {
        return departure_time;
    }

    public String getDeparture_delay() {
        return departure_delay;
    }

    public String getTaxi_out() {
        return taxi_out;
    }

    public String getWheels_off() {
        return wheels_off;
    }

    public String getScheduled_time() {
        return scheduled_time;
    }

    public String getElapsed_time() {
        return elapsed_time;
    }

    public String getAir_time() {
        return air_time;
    }

    public String getDistance() {
        return distance;
    }

    public String getWheels_on() {
        return wheels_on;
    }

    public String getTaxi_in() {
        return taxi_in;
    }

    public String getScheduled_arrival() {
        return scheduled_arrival;
    }

    public String getArrival_time() {
        return arrival_time;
    }

    public String getArrival_delay() {
        return arrival_delay;
    }

    public String getDiverted() {
        return diverted;
    }

    public String getCancelled() {
        return cancelled;
    }

    public String getCancellation_reason() {
        return cancellation_reason;
    }

    public String getAir_system_delay() {
        return air_system_delay;
    }

    public String getSecurity_delay() {
        return security_delay;
    }

    public String getAirline_delay() {
        return airline_delay;
    }

    public String getLate_aircraft_delay() {
        return late_aircraft_delay;
    }

    public String getWeather_delay() {
        return weather_delay;
    }
}
