package com.opower.persistence.jpile.loader;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.opower.persistence.jpile.AbstractIntTestForJPile;
import com.opower.persistence.jpile.infile.events.EventFirePoint;
import com.opower.persistence.jpile.infile.events.FlushEvent;
import com.opower.persistence.jpile.infile.events.SaveEntityEvent;
import com.opower.persistence.jpile.sample.Contact;
import com.opower.persistence.jpile.sample.Customer;
import com.opower.persistence.jpile.sample.Data;
import com.opower.persistence.jpile.sample.ObjectFactory;
import com.opower.persistence.jpile.sample.Product;
import com.opower.persistence.jpile.sample.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.RowMapper;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests object loader for correctness
 *
 * @author amir.raminfar
 */
public class IntTestHierarchicalInfileObjectLoader extends AbstractIntTestForJPile {

    private static final Function<SaveEntityEvent, SaveEntityEventHolder> SAVE_ENTITY_EVENT_INDEX =
            new Function<SaveEntityEvent, SaveEntityEventHolder>() {
                @Override
                public SaveEntityEventHolder apply(SaveEntityEvent event) {
                    return new SaveEntityEventHolder(event);
                }
            };

    private static final Function<FlushEvent, FlushEventHolder> FLUSH_EVENT_INDEX =
            new Function<FlushEvent, FlushEventHolder>() {
                @Override
                public FlushEventHolder apply(FlushEvent event) {
                    return new FlushEventHolder(event);
                }
            };

    private final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public ExpectedException getExpectedException() {
        return this.expectedException;
    }

    @Test
    public void testSingleCustomer() throws Exception {
        // Note, this SimpleDateFormat does NOT match the DATE_TIME_FORMATTER in the InfileDataBuffer.  The database
        // being used with this test does not support milliseconds so we cannot assert with that granularity.  Others
        // using jPile with a later version of MySQL should be able to assert with the granularity of milliseconds.
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Customer expected = ObjectFactory.newCustomer();

        this.hierarchicalInfileObjectLoader.persist(expected);
        this.hierarchicalInfileObjectLoader.flush();
        Map<String, Object> customer = this.jdbcTemplate.queryForMap("select * from customer");
        Map<String, Object> contact = this.jdbcTemplate.queryForMap("select * from contact");
        Map<String, Object> phone = this.jdbcTemplate.queryForMap("select * from contact_phone");
        List<Map<String, Object>> products = this.jdbcTemplate.queryForList("select * from product");
        Map<String, Object> supplier = this.jdbcTemplate.queryForMap("select * from supplier");

        assertEquals(simpleDateFormat.format(expected.getLastSeenOn()), simpleDateFormat.format(customer.get("last_seen_on")));
        assertEquals(expected.getType().ordinal(), customer.get("type"));
        assertEquals(expected.getId(), customer.get("id"));
        assertEquals(expected.getId(), contact.get("customer_id"));

        Contact expectedContact = Iterables.getOnlyElement(expected.getContacts());
        assertEquals(expectedContact.getContactPK().getFirstName(), contact.get("first_name"));
        assertEquals(expectedContact.getLastName(), contact.get("last_name"));
        assertEquals(expected.getId(), phone.get("customer_id"));
        assertEquals(expectedContact.getPhone(), phone.get("phone"));
        assertEquals(expectedContact.getType().name(), contact.get("type"));
        assertEquals(expectedContact.getAddress().getStreetNumber(), contact.get("street_number"));
        assertEquals(expectedContact.getAddress().getStreet(), contact.get("street"));
        assertEquals(expectedContact.getAddress().getCity(), contact.get("city"));
        assertEquals(expectedContact.getAddress().getState(), contact.get("state"));
        assertEquals(expectedContact.getAddress().getZipCode(), contact.get("zip_code"));
        assertEquals(expected.getProducts().size(), products.size());

        for (int i = 0, productsSize = expected.getProducts().size(); i < productsSize; i++) {
            Product expectedProduct = expected.getProducts().get(i);
            Map<String, Object> actualMap = products.get(i);
            assertEquals(expectedProduct.getId(), actualMap.get("id"));
            assertEquals(expected.getId().intValue(), actualMap.get("customer_id"));
            assertEquals(expectedProduct.getSupplier().getId().intValue(), actualMap.get("supplier_id"));
            assertEquals(expectedProduct.getTitle(), actualMap.get("title"));
            assertEquals(expectedProduct.getDescription(), actualMap.get("description"));
            assertEquals(expectedProduct.getPrice().doubleValue(), actualMap.get("price"));
            assertEquals(simpleDateFormat.format(expectedProduct.getPurchasedOn()),
                         simpleDateFormat.format(actualMap.get("purchased_on")));
            assertEquals(expectedProduct.getPackaging().ordinal(), actualMap.get("packaging"));
            assertEquals(expectedProduct.getSupplier().getAddress().getStreetNumber(), supplier.get("street_number"));
            assertEquals(expectedProduct.getSupplier().getAddress().getStreet(), supplier.get("street"));
            assertEquals(expectedProduct.getSupplier().getAddress().getCity(), supplier.get("city"));
            assertEquals(expectedProduct.getSupplier().getAddress().getState(), supplier.get("state"));
            assertEquals(expectedProduct.getSupplier().getAddress().getZipCode(), supplier.get("zip_code"));
        }
    }

    /**
     * Test that updating rows works. Persists two rows with the same primary key. Asserts that the second persist wins.
     * @throws Exception
     */
    @Test
    public void testMultipleCustomers() throws Exception {
        Customer customer1 = ObjectFactory.newCustomer();
        customer1.setId(1L);
        customer1.setType(Customer.Type.RESIDENTIAL);

        Customer customer2 = ObjectFactory.newCustomer();
        customer2.setId(1L);
        customer2.setType(Customer.Type.SMALL_BUSINESS);

        this.hierarchicalInfileObjectLoader.setUseReplace(true);
        this.hierarchicalInfileObjectLoader.persist(customer1);
        this.hierarchicalInfileObjectLoader.persist(customer2);

        this.hierarchicalInfileObjectLoader.flush();

        Map<String, Object> customers = this.jdbcTemplate.queryForMap("select * from customer");
        assertEquals(Customer.Type.SMALL_BUSINESS.ordinal(), customers.get("type"));
    }

    @Test
    public void testHundredCustomers() {
        for (int i = 0; i < 100; i++) {
            this.hierarchicalInfileObjectLoader.persist(ObjectFactory.newCustomer());
        }
    }

    @Test
    public void testBinaryDataToHex() throws NoSuchAlgorithmException {
        String string = "Data to be inserted";
        byte[] md5 = toMd5(string);
        Data data = new Data();
        data.setName(string);
        data.setMd5(md5);

        this.hierarchicalInfileObjectLoader.persist(data);
        this.hierarchicalInfileObjectLoader.flush();

        Data actual = this.jdbcTemplate.queryForObject("select * from binary_data", new RowMapper<Data>() {
            @Override
            public Data mapRow(ResultSet rs, int rowNum) throws SQLException {
                Data data = new Data();
                data.setId(rs.getLong("id"));
                data.setName(rs.getString("name"));
                data.setMd5(rs.getBytes("md5"));
                return data;
            }
        });

        assertTrue(Arrays.equals(md5, actual.getMd5()));
    }

    @Test
    @SuppressWarnings({"rawtypes", "deprecation"}) // Testing deprecated method
    public void testClassesToIgnore() {
        this.hierarchicalInfileObjectLoader.setClassesToIgnore(ImmutableSet.<Class>of(Customer.class));

        Customer customer = ObjectFactory.newCustomer();
        this.hierarchicalInfileObjectLoader.persist(customer);

        assertNull(customer.getId());
    }

    @Test
    public void testIgnoredClasses() {
        this.hierarchicalInfileObjectLoader.setIgnoredClasses(ImmutableSet.<Class<?>>of(Customer.class));

        Customer customer = ObjectFactory.newCustomer();
        this.hierarchicalInfileObjectLoader.persist(customer);

        assertNull(customer.getId());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testEventCallback() {
        HierarchicalInfileObjectLoader.CallBack callBack = mock(HierarchicalInfileObjectLoader.CallBack.class);
        this.hierarchicalInfileObjectLoader.setEventCallback(callBack);

        Customer customer = ObjectFactory.newCustomer();
        this.hierarchicalInfileObjectLoader.persist(customer);

        verify(callBack, times(1)).onBeforeSave(customer);
        verify(callBack, times(1)).onAfterSave(customer);
    }

    /**
     * This test verifies that appropriate events are sent while adding entity to buffer and flushing it.
     */
    @Test
    public void testEvents() {
        final Customer customer = ObjectFactory.newCustomer();
        final int expectedNumberOfSaveEntityEvents = 14;
        final int expectedNumberOfFlushEvents = 10;
        final TestListener testListener = mock(TestListener.class);
        try {
            this.hierarchicalInfileObjectLoader.subscribe(testListener);
            this.hierarchicalInfileObjectLoader.persist(customer);
            this.hierarchicalInfileObjectLoader.flush();

            ArgumentCaptor<SaveEntityEvent> saveEntityEventCaptor = ArgumentCaptor.forClass(SaveEntityEvent.class);
            verify(testListener, times(expectedNumberOfSaveEntityEvents)).dispatch(saveEntityEventCaptor.capture());

            // Check for duplicates by using uniqueIndex
            Set<SaveEntityEventHolder> saveEntityEventIndex = Maps.uniqueIndex(saveEntityEventCaptor.getAllValues(),
                    SAVE_ENTITY_EVENT_INDEX).keySet();
            assertEquals(expectedNumberOfSaveEntityEvents, saveEntityEventIndex.size());

            assertSaveEntityEventFired(saveEntityEventIndex, new SaveEntityEvent(this, EventFirePoint.BEFORE, customer));
            assertSaveEntityEventFired(saveEntityEventIndex, new SaveEntityEvent(this, EventFirePoint.AFTER, customer));

            for (Contact contact : customer.getContacts()) {
                assertSaveEntityEventFired(saveEntityEventIndex, new SaveEntityEvent(this, EventFirePoint.BEFORE, contact));
                assertSaveEntityEventFired(saveEntityEventIndex, new SaveEntityEvent(this, EventFirePoint.AFTER, contact));
            }

            for (Product product : customer.getProducts()) {
                assertSaveEntityEventFired(saveEntityEventIndex, new SaveEntityEvent(this, EventFirePoint.BEFORE, product));
                assertSaveEntityEventFired(saveEntityEventIndex, new SaveEntityEvent(this, EventFirePoint.AFTER, product));
                Supplier supplier = product.getSupplier();
                assertSaveEntityEventFired(saveEntityEventIndex, new SaveEntityEvent(this, EventFirePoint.BEFORE, supplier));
                assertSaveEntityEventFired(saveEntityEventIndex, new SaveEntityEvent(this, EventFirePoint.AFTER, supplier));
            }

            ArgumentCaptor<FlushEvent> flushEventCaptor = ArgumentCaptor.forClass(FlushEvent.class);
            verify(testListener, times(expectedNumberOfFlushEvents)).dispatch(flushEventCaptor.capture());

            // Check for duplicates by using uniqueIndex
            Set<FlushEventHolder> flushEventIndex = Maps.uniqueIndex(flushEventCaptor.getAllValues(),
                    FLUSH_EVENT_INDEX).keySet();
            assertEquals(expectedNumberOfFlushEvents, flushEventIndex.size());

            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.BEFORE, Customer.class, "customer"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.AFTER, Customer.class, "customer"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.BEFORE, Contact.class, "contact"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.AFTER, Contact.class, "contact"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.BEFORE, Contact.class, "contact_phone"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.AFTER, Contact.class, "contact_phone"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.BEFORE, Product.class, "product"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.AFTER, Product.class, "product"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.BEFORE, Supplier.class, "supplier"));
            assertFlushEventFired(flushEventIndex, new FlushEvent(this, EventFirePoint.AFTER, Supplier.class, "supplier"));
        }
        finally {
            this.hierarchicalInfileObjectLoader.unsubscribe(testListener);
        }
    }

    private static void assertSaveEntityEventFired(Set<SaveEntityEventHolder> index, SaveEntityEvent event) {
        assertTrue(index.contains(new SaveEntityEventHolder(event)));
    }

    private static void assertFlushEventFired(Set<FlushEventHolder> index, FlushEvent event) {
        assertTrue(index.contains(new FlushEventHolder(event)));
    }

    /**
     * Test implementation of listener.
     */
    private static class TestListener {
        @Subscribe public void dispatch(@SuppressWarnings("unused") SaveEntityEvent event) {}
        @Subscribe public void dispatch(@SuppressWarnings("unused") FlushEvent event) {}
    }

    /**
     * Event holder which has custom {@link #equals(Object)} and {@link #hashCode()} implementations.
     */
    private static class SaveEntityEventHolder {

        final SaveEntityEvent event;

        SaveEntityEventHolder(SaveEntityEvent event) {
            this.event = event;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SaveEntityEventHolder that = (SaveEntityEventHolder) o;
            return Objects.equal(this.event.getEntity(), that.event.getEntity())
                    && this.event.getFirePoint() == that.event.getFirePoint();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.event.getEntity(),
                    this.event.getFirePoint());
        }
    }

    /**
     * Event holder which has custom {@link #equals(Object)} and {@link #hashCode()} implementations.
     */
    private static class FlushEventHolder {

        final FlushEvent event;

        FlushEventHolder(FlushEvent event) {
            this.event = event;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FlushEventHolder that = (FlushEventHolder) o;
            return Objects.equal(this.event.getEntityClass(), that.event.getEntityClass())
                    && Objects.equal(this.event.getTableName(), that.event.getTableName())
                    && this.event.getFirePoint() == that.event.getFirePoint();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.event.getEntityClass(),
                    this.event.getTableName(),
                    this.event.getFirePoint());
        }

    }

    /**
     * Tests that number of events isn't doubled
     * if {@link HierarchicalInfileObjectLoader#subscribe(Object)} listener few times.
     */
    @Test
    public void testSubscribeFewTimes() {
        final TestListener testListener = mock(TestListener.class);
        try {
            this.hierarchicalInfileObjectLoader.subscribe(testListener);
            this.hierarchicalInfileObjectLoader.subscribe(testListener);

            testEvents();

            final int expectedNumberOfSaveEntityEvents = 14;
            ArgumentCaptor<SaveEntityEvent> saveEntityEventCaptor = ArgumentCaptor.forClass(SaveEntityEvent.class);
            verify(testListener, times(expectedNumberOfSaveEntityEvents)).dispatch(saveEntityEventCaptor.capture());

            final int expectedNumberOfFlushEvents = 10;
            ArgumentCaptor<FlushEvent> flushEventCaptor = ArgumentCaptor.forClass(FlushEvent.class);
            verify(testListener, times(expectedNumberOfFlushEvents)).dispatch(flushEventCaptor.capture());
        }
        finally {
            this.hierarchicalInfileObjectLoader.unsubscribe(testListener);
        }
    }

    /**
     * Tests that no events are received
     * if listener was {@link HierarchicalInfileObjectLoader#subscribe(Object)}d
     * and {@link HierarchicalInfileObjectLoader#unsubscribe(Object)}s.
     */
    @Test
    public void testUnSubscribe() {
        final TestListener testListener = mock(TestListener.class);
        this.hierarchicalInfileObjectLoader.subscribe(testListener);
        this.hierarchicalInfileObjectLoader.unsubscribe(testListener);

        testEvents();

        final int expectedNumberOfSaveEntityEvents = 0;
        ArgumentCaptor<SaveEntityEvent> saveEntityEventCaptor = ArgumentCaptor.forClass(SaveEntityEvent.class);
        verify(testListener, times(expectedNumberOfSaveEntityEvents)).dispatch(saveEntityEventCaptor.capture());

        final int expectedNumberOfFlushEvents = 0;
        ArgumentCaptor<FlushEvent> flushEventCaptor = ArgumentCaptor.forClass(FlushEvent.class);
        verify(testListener, times(expectedNumberOfFlushEvents)).dispatch(flushEventCaptor.capture());
    }

    /**
     * Tests that expected exception is thrown if {@link HierarchicalInfileObjectLoader#unsubscribe(Object)}
     * listener which wasn't {@link HierarchicalInfileObjectLoader#subscribe(Object)}d.
     */
    @Test
    public void testUnSubscribeIfNotSubscribed() {
        final TestListener testListener = mock(TestListener.class);
        this.expectedException.expect(IllegalArgumentException.class);
        this.expectedException.expectMessage(
                String.format("missing event handler for an annotated method. Is %s registered?", testListener));
        this.hierarchicalInfileObjectLoader.unsubscribe(testListener);
    }

    @Test
    public void testUtf8() {
        Contact expected = ObjectFactory.newContact();
        expected.getContactPK().setFirstName("\u304C\u3126");
        expected.setLastName("ががががㄦ");

        this.hierarchicalInfileObjectLoader.persist(expected);
        this.hierarchicalInfileObjectLoader.flush();

        Map<String, Object> actual = this.jdbcTemplate.queryForMap("select * from contact");

        assertEquals("がㄦ", actual.get("first_name"));
        assertEquals("ががががㄦ", actual.get("last_name"));
    }

    /**
     * Verify that all of the special characters in the {@link com.opower.persistence.jpile.infile.InfileDataBuffer} are
     * correctly escaped and stored.
     */
    @Test
    public void testAppendStringEscapesSpecialCharacters() {
        Contact expected = ObjectFactory.newContact();
        expected.getContactPK().setFirstName("D\ba\nv\ri\td\0D\\D\u001A");

        this.hierarchicalInfileObjectLoader.setUseReplace(true);
        this.hierarchicalInfileObjectLoader.persist(expected);
        this.hierarchicalInfileObjectLoader.flush();

        Map<String, Object> actual = this.jdbcTemplate.queryForMap("select * from contact");

        assertEquals("D\ba\nv\ri\td\0D\\D\u001A", actual.get("first_name"));
    }

    private byte[] toMd5(String s) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(s.getBytes());
        return md.digest();
    }
}
