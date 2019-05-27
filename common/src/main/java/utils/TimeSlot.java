package utils;

public enum TimeSlot {

    MORNING("Morning"),
    AFTERNOON("Afternoon"),
    EVENING("Evening"),
    NIGHT("Night");

    private final String description;

    TimeSlot(final String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }

    public static TimeSlot getTimeSlot(final String time) {
        final int hour = Integer.parseInt(time.substring(0, 2));
        if (hour >= 0 && hour < 6) return TimeSlot.NIGHT;
        if (hour >= 6 && hour < 12) return TimeSlot.MORNING;
        if (hour >= 12 && hour < 18) return TimeSlot.AFTERNOON;
        return TimeSlot.EVENING;
    }

    public static TimeSlot getTimeSlotFromDescription(final String description) {
        switch (description) {
            case "Morning": return TimeSlot.MORNING;
            case "Afternoon": return TimeSlot.AFTERNOON;
            case "Evening": return TimeSlot.EVENING;
            case "Night": return TimeSlot.NIGHT;
            default: return TimeSlot.MORNING;
        }
    }

    public static TimeSlot getTimeSlot(final int ordinal) {
        return TimeSlot.values()[ordinal];
    }
}
