package com.opower.persistence.jpile.loader;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.opower.persistence.jpile.AbstractIntTestForJPile;
import com.opower.persistence.jpile.sample.Address;
import com.opower.persistence.jpile.sample.Contact;
import com.opower.persistence.jpile.sample.Customer;
import com.opower.persistence.jpile.sample.ObjectFactory;
import com.opower.persistence.jpile.sample.Product;
import com.opower.persistence.jpile.sample.Supplier;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.annotation.IfProfileValue;

import static junit.framework.Assert.assertEquals;

/**
 * Tests the performance of MySQL with prepared statements and Hibernate. This test is disabled by default because it
 * takes a while to run.
 *
 * @author amir.raminfar
 */
@IfProfileValue(name = "performance", value = "true")
public class IntTestPerformanceHierarchicalInfileObjectLoader extends AbstractIntTestForJPile {
    private static final String CUSTOMER_SQL = "insert into customer (last_seen_on, type) values (?, ?)";
    private static final String SUPPLIER_SQL = "insert supplier (name, street_number, street, city, state, zip_code) " +
            "values (?, ?, ?, ?, ?, ?)";
    private static final String PRODUCT_SQL
            = "insert into product (customer_id, purchased_on, title, description, price, packaging, supplier_id) " +
            "values (?, ?, ?, ?, ?, ?, ?)";
    private static final String CONTACT_SQL = "insert into contact " +
            "(customer_id, first_name, last_name, street_number, street, city, state, zip_code) " +
            "values (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String CONTACT_PHONE_SQL = "insert contact_phone (customer_id, phone) values (?, ?)";
    private static final int CUSTOMERS_TO_GENERATE = 25000;

    private Customer[] customers;

    @Before
    public void generateCustomers() throws Exception {
        customers = new Customer[CUSTOMERS_TO_GENERATE];
        for (int i = 0; i < CUSTOMERS_TO_GENERATE; i++) {
            customers[i] = ObjectFactory.newCustomer();
        }
    }

    @After
    public void assertNumberOfCustomer() {
        Integer numCustomers = jdbcTemplate.queryForObject("select count(*) from customer", Integer.class);
        assertEquals(CUSTOMERS_TO_GENERATE, numCustomers.intValue());
    }

    @Test
    public void testWithPreparedStatement() throws SQLException {
        final PreparedStatement customerStatement = connection.prepareStatement(CUSTOMER_SQL);
        final PreparedStatement supplierStatement = connection.prepareStatement(SUPPLIER_SQL);
        final PreparedStatement productStatement = connection.prepareStatement(PRODUCT_SQL);
        final PreparedStatement contactStatement = connection.prepareStatement(CONTACT_SQL);
        final PreparedStatement phoneStatement = connection.prepareStatement(CONTACT_PHONE_SQL);

        doWithInTimedBlock(new Runnable() {
            @Override
            public void run() {
                try {
                    for (long i = 0, customersLength = customers.length; i < customersLength; i++) {
                        // Use this as the autoincrement id for Customers and Suppliers
                        long id = i + 1;

                        Customer customer = customers[((int) i)];
                        customer.setId(id);

                        customerStatement.clearParameters();
                        supplierStatement.clearParameters();
                        productStatement.clearParameters();
                        contactStatement.clearParameters();
                        phoneStatement.clearParameters();

                        writeCustomer(customerStatement, customer);
                        customerStatement.executeUpdate();

                        for (Product product : customer.getProducts()) {
                            writeSupplier(supplierStatement, product.getSupplier());
                            supplierStatement.executeUpdate();

                            writeProduct(productStatement, customer, product, id);
                            productStatement.executeUpdate();
                        }

                        writeContact(contactStatement, customer);
                        contactStatement.executeUpdate();

                        writeContactPhone(phoneStatement, customer);
                        phoneStatement.executeUpdate();
                    }
                }
                catch (SQLException e) {
                    throw Throwables.propagate(e);
                }
            }
        }, "Prepared Statements");

        customerStatement.close();
        supplierStatement.close();
        productStatement.close();
        contactStatement.close();
        phoneStatement.close();
    }

    @Test
    public void testWithHibernate() {
        final SessionFactory sessionFactory = new Configuration().configure()
                                                                 .addAnnotatedClass(Customer.class)
                                                                 .addAnnotatedClass(Contact.class)
                                                                 .addAnnotatedClass(Product.class)
                                                                 .addAnnotatedClass(Supplier.class)
                                                                 .buildSessionFactory();

        doWithInTimedBlock(new Runnable() {
            public void run() {
                Session session = sessionFactory.openSession();
                session.beginTransaction();
                for (Customer customer : customers) {
                    session.save(customer);
                }
                session.flush();
                session.getTransaction().commit();
                session.close();
            }
        }, "Hibernate");
    }

    @Test
    public void testWithJPile() {
        doWithInTimedBlock(new Runnable() {
            @Override
            public void run() {
                hierarchicalInfileObjectLoader.persist(customers[0], (Object[]) customers);
                hierarchicalInfileObjectLoader.flush();
            }
        }, "jPile");
    }

    private static void writeContactPhone(PreparedStatement contactPhoneStatement, Customer customer) throws SQLException {
        contactPhoneStatement.setLong(1, customer.getId());
        contactPhoneStatement.setString(2, Iterables.getOnlyElement(customer.getContacts()).getPhone());
    }

    private static void writeContact(PreparedStatement contactStatement, Customer customer) throws SQLException {
        Address address = Iterables.getOnlyElement(customer.getContacts()).getAddress();

        contactStatement.setLong(1, customer.getId());
        contactStatement.setString(2, Iterables.getOnlyElement(customer.getContacts()).getContactPK().getFirstName());
        contactStatement.setString(3, Iterables.getOnlyElement(customer.getContacts()).getLastName());
        contactStatement.setString(4, address.getStreetNumber());
        contactStatement.setString(5, address.getStreet());
        contactStatement.setString(6, address.getCity());
        contactStatement.setString(7, address.getState());
        contactStatement.setString(8, address.getZipCode());
    }

    private static void writeProduct(PreparedStatement productStatement, Customer customer, Product product, long supplierId)
            throws SQLException {
        productStatement.setLong(1, customer.getId());
        productStatement.setDate(2, new Date(product.getPurchasedOn().getTime()));
        productStatement.setString(3, product.getTitle());
        productStatement.setString(4, product.getDescription());
        productStatement.setBigDecimal(5, product.getPrice());
        productStatement.setObject(6, product.getPackaging().ordinal());
        productStatement.setLong(7, supplierId);
    }

    private static void writeCustomer(PreparedStatement customerStatement, Customer customer) throws SQLException {
        customerStatement.setDate(1, new Date(customer.getLastSeenOn().getTime()));
        customerStatement.setObject(2, customer.getType().ordinal());
    }

    private static void writeSupplier(PreparedStatement supplierStatement, Supplier supplier) throws SQLException {
        Address address = supplier.getAddress();

        supplierStatement.setString(1, supplier.getName());
        supplierStatement.setString(2, address.getStreetNumber());
        supplierStatement.setString(3, address.getStreet());
        supplierStatement.setString(4, address.getCity());
        supplierStatement.setString(5, address.getState());
        supplierStatement.setString(6, address.getZipCode());
    }

    private static void doWithInTimedBlock(Runnable runnable, String name) {
        long start = System.nanoTime();
        runnable.run();
        long elapsed = System.nanoTime() - start;
        System.out.println(Strings.repeat("=", 100));
        System.out.printf("Total time to save %d customers was %d seconds with %s.%n",
                CUSTOMERS_TO_GENERATE,
                TimeUnit.NANOSECONDS.toSeconds(elapsed),
                name);
        System.out.printf("Throughput for %s was %d objects/second%n",
                name,
                CUSTOMERS_TO_GENERATE / TimeUnit.NANOSECONDS.toSeconds(elapsed));
        System.out.println(Strings.repeat("=", 100));
        System.out.println();
    }
}
