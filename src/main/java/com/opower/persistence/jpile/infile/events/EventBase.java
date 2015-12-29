package com.opower.persistence.jpile.infile.events;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for events.
 *
 * @author phillip-michailov
 */
public class EventBase {

    private final Object source;
    private final EventFirePoint firePoint;

    /**
     * Constructor.
     *
     * @param source object that fired this event.
     * @param firePoint point in code path where this event occurred.
     */
    public EventBase(Object source, EventFirePoint firePoint) {
        this.source = checkNotNull(source, "Source cannot be null");
        this.firePoint = checkNotNull(firePoint, "Fire point cannot be null");
    }

    /**
     * @return object that fired this event.
     */
    public Object getSource() {
        return this.source;
    }

    /**
     * @return point in code path where this event occurred.
     */
    public EventFirePoint getFirePoint() {
        return this.firePoint;
    }
}
