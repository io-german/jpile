package com.opower.persistence.jpile.sample;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.SecondaryTable;
import javax.persistence.SecondaryTables;
import javax.persistence.Table;

import java.io.Serializable;

/**
 * A sample pojo object for testing
 *
 * @author amir.raminfar
 */
@Entity
@Table(name = "contact")
@SecondaryTables(@SecondaryTable(name = "contact_phone", pkJoinColumns = {@PrimaryKeyJoinColumn(name = "customer_id"),
        @PrimaryKeyJoinColumn(name = "first_name")}))
public class Contact {
    /**
     * The type of contact.
     */
    public enum Type {
        PRIMARY,
        SECONDARY
    }

    private ContactPK contactPK;
    private String lastName;
    private String phone;
    private Type type;
    private Address address;

    @EmbeddedId
    public ContactPK getContactPK() {
        return this.contactPK;
    }

    public void setContactPK(ContactPK contactPK) {
        this.contactPK = contactPK;
    }

    /**
     * EmbeddedId class representing a composite primary key.
     */
    @Embeddable
    public static class ContactPK implements Serializable {
        private static final long serialVersionUID = 1L;

        private Customer customer;
        private String firstName;

        /* Default constructor for Hibernate */
        public ContactPK() {
        }

        public ContactPK(Customer customer, String firstName) {
            this.customer = customer;
            this.firstName = firstName;
        }

        @ManyToOne
        @JoinColumn(name = "customer_id")
        @GeneratedValue(generator = "foreign")
        @GenericGenerator(name = "foreign", strategy = "foreign",
                parameters = {@Parameter(name = "property", value = "customer")})
        public Customer getCustomer() {
            return customer;
        }

        public void setCustomer(Customer customer) {
            this.customer = customer;
        }

        @Column(name = "first_name")
        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }
    }

    @Column(name = "phone", table = "contact_phone")
    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    @Column(name = "last_name")
    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Enumerated(EnumType.STRING)
    @Column(name = "type")
    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Embedded
    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }
}
