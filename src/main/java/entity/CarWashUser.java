package entity;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Represents a subscriber who used carwash
 */
public class CarWashUser {
    /**
     * Primary key (subscriber number).
     */
    @QuerySqlField(index = true)
    private long subsKey;

    /**
     * Concurrent index(1-enemy, 0-friend).
     */
    @QuerySqlField
    private int concInd;

    /**
     * Friend CarWash name.
     */
    @QuerySqlField
    private String name;


    /**
     * Constructs a  instance.
     *
     * @param subsKey Subscriber number.
     * @param concInd Carwash concurrent index.
     * @param name    Carwash name.
     */
    public CarWashUser(long subsKey, int concInd, String name) {
        if (concInd != 0 && concInd != 1)
            throw new IllegalArgumentException("concInd should be 0 or 1, given: " + concInd);

        this.subsKey = subsKey;
        this.concInd = concInd;
        this.name = name;
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
     * Gets carwash concurrent index.
     *
     * @return Carwash concurrent index.
     */
    public int getConcInd() {
        return concInd;
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
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "CarWashUser [subsKey=" + subsKey +
                ", concInd=" + concInd +
                ", name=" + name + ']';
    }
}
