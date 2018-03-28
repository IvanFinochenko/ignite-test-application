package entity;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Represents a carwash entity
 */
public class CarWash {
    /**
     * Primary key (carwash number).
     */
    @QuerySqlField(index = true)
    private long subsKey;

    /**
     * CarWash name.
     */
    @QuerySqlField
    private String name;

    /**
     * The place where carwash is located.
     */
    @QuerySqlField
    private String place;

    /**
     * Concurrent index(1-enemy, 0-friend).
     */
    @QuerySqlField
    private int cuncInd;

    /**
     * Constructs a  instance.
     *
     * @param subsKey Carwash number.
     * @param name    Carwash name.
     * @param place   Carwash location.
     * @param concInd Carwash concurrent index.
     */
    public CarWash(long subsKey, String name, String place, int concInd) {
        if (concInd != 0 && concInd != 1)
            throw new IllegalArgumentException("cuncInd should be 0 or 1, given: " + concInd);

        this.subsKey = subsKey;
        this.name = name;
        this.place = place;
        this.cuncInd = concInd;
    }

    /**
     * Gets carwash number.
     *
     * @return Carwash number.
     */
    public long getSubsKey() {
        return subsKey;
    }

    /**
     * Gets carwash name.
     *
     * @return Carwash name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets carwash place.
     *
     * @return Carwash place.
     */
    public String getPlace() {
        return place;
    }

    /**
     * Gets carwash concurrent index.
     *
     * @return Carwash concurrent index.
     */
    public int getConcInd() {
        return cuncInd;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Carwash [subsKey=" + subsKey +
                ", name=" + name +
                ", place=" + place +
                ", concInd=" + cuncInd + ']';
    }
}