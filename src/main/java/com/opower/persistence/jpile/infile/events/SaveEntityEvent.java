package com.opower.persistence.jpile.infile.events;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This event indicates that object was saved to the infile buffer.
 *
 * @author phillip-michailov
 */
public class SaveEntityEvent extends EventBase {

    private final Object entity;

    /**
     * Constructor
     *
     * @param source object that fired this event.
     * @param firePoint point in code path where this event occurred.
     * @param entity entity instance that was saved to buffer.
     */
    public SaveEntityEvent(Object source, EventFirePoint firePoint, Object entity) {
        super(source, firePoint);
        this.entity = checkNotNull(entity, "Entity cannot be null");
    }

    /**
     * @return entity instance that was saved to buffer.
     */
    public Object getEntity() {
        return this.entity;
    }
}
