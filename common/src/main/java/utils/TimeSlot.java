package utils;

/**
 * Enumeration containing the four time slots considered in the AirportsJob
 */
public enum TimeSlot {

    MORNING("Morning"),
    AFTERNOON("Afternoon"),
    EVENING("Evening"),
    NIGHT("Night");

    //the description of the time slot
    private final String description;

    TimeSlot(final String description) {
        this.description = description;
    }

    /**
     * Returns the description of a time slot
     * @return time slot description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Converts the time string to the related time slot
     * @param time time string in hhmm format
     * @return the time slot
     */
    public static TimeSlot getTimeSlot(final String time) {
        final int hour = Integer.parseInt(time.substring(0, 2));
        if (hour >= 0 && hour < 6) return TimeSlot.NIGHT;
        if (hour >= 6 && hour < 12) return TimeSlot.MORNING;
        if (hour >= 12 && hour < 18) return TimeSlot.AFTERNOON;
        return TimeSlot.EVENING;
    }

    /**
     * Converts the enum ordinal to the related time slot
     * @param ordinal ordinal enum
     * @return the time slot
     */
    public static TimeSlot getTimeSlot(final int ordinal) {
        return TimeSlot.values()[ordinal];
    }
}
