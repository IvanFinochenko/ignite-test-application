package entity;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.time.LocalDateTime;

/**
 * Represents an action of calling
 */
public class Call {
    /**
     * Count do primary key in table
     */
    public static int INSTANCE_COUNT;

    /**
     * Who called.
     */
    @QuerySqlField(index = true)
    private long subsFrom;

    /**
     * Whom was called.
     */
    private long subsTo;

    /**
     * Duration of action in sec.
     */
    private int dur;

    /**
     * Start time. (yyyy-MM-dd hh:ss)
     */
    @QuerySqlField(index = true)
    private LocalDateTime startTime;

    /**
     * Constructs a  instance.
     *
     * @param subsFrom  Call initiator.
     * @param subsTo    Call target.
     * @param dur       Call duration.
     * @param startTime Call start time.
     */
    public Call(long subsFrom, long subsTo, int dur, LocalDateTime startTime) {
        this.subsFrom = subsFrom;
        this.subsTo = subsTo;
        this.dur = dur;
        this.startTime = startTime;
    }

    /**
     * Gets subscriber number who called.
     *
     * @return Subscriber number.
     */
    public long getSubsFrom() {
        return subsFrom;
    }

    /**
     * Gets subscriber number whom was called..
     *
     * @return Subscriber number.
     */
    public long getSubsTo() {
        return subsTo;
    }

    /**
     * Gets call duration.
     *
     * @return Duration.
     */
    public int getDur() {
        return dur;
    }

    /**
     * Gets start time.
     *
     * @return Start time.
     */
    public LocalDateTime getStartTime() {
        return startTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Call [subsFrom=" + subsFrom +
                ", subsTo=" + subsTo +
                ", dur=" + dur +
                ", startTime=" + startTime + ']';
    }
}
