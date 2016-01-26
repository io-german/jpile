package com.opower.persistence.jpile.infile.events;

import com.google.common.eventbus.Subscribe;
import com.opower.persistence.jpile.loader.HierarchicalInfileObjectLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Adapter for {@link HierarchicalInfileObjectLoader.CallBack} for backward compatibility.
 *
 * @author phillip-michailov
 */
@SuppressWarnings("deprecation")
public class SaveEntityEventAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveEntityEventAdapter.class);

    private final HierarchicalInfileObjectLoader.CallBack callBack;

    public SaveEntityEventAdapter(HierarchicalInfileObjectLoader.CallBack callBack) {
        this.callBack = checkNotNull(callBack, "callback");
    }

    @Subscribe
    @SuppressWarnings("unused")
    public void dispatch(SaveEntityEvent event) {
        EventFirePoint firePoint = event.getFirePoint();
        Object entity = event.getEntity();
        switch (firePoint) {
            case BEFORE:
                this.callBack.onBeforeSave(entity);
                break;
            case AFTER:
                this.callBack.onAfterSave(entity);
                break;
            default:
                LOGGER.warn("Unknown fire point: {}", firePoint);
        }
    }
}
