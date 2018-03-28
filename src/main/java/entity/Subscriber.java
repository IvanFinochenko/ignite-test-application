package entity;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.time.LocalDate;

/**
 * Represents a subscriber entity
 */
public class Subscriber {
    /**
     * Primary key (subscriber number).
     */
    @QuerySqlField(index = true)
    private long subsKey;

    /**
     * Subscriber name (can be first + middle + last).
     */
    @QuerySqlField
    private String name;

    /**
     * The place where subscriber was registered.
     */
    @QuerySqlField
    private String place;

    /**
     * When was subscriber registered (yyyy-MM-dd).
     */
    @QuerySqlField(index = true)
    private LocalDate timeKey;

    /**
     * Constructs a  instance.
     *
     * @param subsKey Subscriber number.
     * @param name    Subscriber name.
     * @param place   Subscriber registration place.
     * @param timeKey Subscriber registration time.
     */
    public Subscriber(long subsKey, String name, String place, LocalDate timeKey) {
        this.subsKey = subsKey;
        this.name = name;
        this.place = place;
        this.timeKey = timeKey;
    }

    /**
     * Gets subscriber number.
     *
     * @return Subscriber number.
     */
    public long getSubsKey() {
        return subsKey;
    }

    /**
     * Gets subscriber name.
     *
     * @return Subscriber name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets subscriber code.
     *
     * @return Subscriber code.
     */
    public String getPlace() {
        return place;
    }

    /**
     * Gets subscriber registration date.
     *
     * @return Subscriber registration date.
     */
    public LocalDate getTimeKey() {
        return timeKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Subscriber [subsKey=" + subsKey +
                ", name=" + name +
                ", place=" + place +
                ", timeKey=" + timeKey + ']';
    }
}