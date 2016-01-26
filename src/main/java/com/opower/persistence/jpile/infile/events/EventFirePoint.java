package com.opower.persistence.jpile.infile.events;

/**
 * A point in the code path where an event was fired.
 * For now it can be either `before` some action or 'after' it.
 *
 * @author phillip-michailov
 */
public enum EventFirePoint {
    BEFORE, AFTER
}
