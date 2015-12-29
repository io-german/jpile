[![Build Status](https://travis-ci.org/opower/jpile.svg?branch=master)](https://travis-ci.org/opower/jpile)

# What is jPile?

A project developed at Opower that uses `javax.persistence` annotations to load objects in MySQL using its infile stream format. This component is meant for importing large amount of data at a high throughput rate. It supports many of the same features Hibernate supports except with up to _10x_ performance gain. So for example, if your data takes 60 seconds to be imported into a MySQL database, with jPile it would only take 6 seconds! You don't have to change anything on your model objects. jPile will read the persistence annotations automatically at start up.


# What annotations are supported?

The following annotations are supported:

* @Table
* @SecondaryTable
* @SecondaryTables
* @Embedded
* @EmbeddedId
* @Id
* @Column
* @OneToMany
* @ManyToOne
* @OneToOne
* @JoinColumn
* @PrimaryKeyJoinColumn
* @GeneratedValue
* @Temporal
* @Enumerated


# How does jPile handle ids?

jPile cannot rely on MySQL `auto_generated` option. Typical database operations save a new row and fetch the last auto generated id.  This is not possible when flushing an infile stream to the database. Instead jPile tries to generate its own auto generated ids for any column definition that has `@GeneratedValue(strategy = GenerationType.AUTO)`.

# Does jPile update entities?

jPile allows the client to configure whether entities are updated when inserting into an existing row with a duplicate primary/unique key. There is a slight decrease in performance when using this feature: persisting entities takes around 30-40% longer. Performance of replacing entities decreases, as the number of rows that need to be updated increases.

# How do I run the tests?

jPile needs a local MySQL running and Apache Maven. Create a new database schema called 'jpile' using `CREATE DATABASE jpile CHARACTER SET utf8 COLLATE utf8_general_ci`. The test classes use `root` with no password to login. The username and password is located in `AbstractIntTestForJPile` class.

All test cases will automatically create and drop the required tables for integration tests. After creating the local database, you should be able to run `mvn clean install` to run all the tests and install locally.

# What do I do if I find a bug?

The project is still under development. One of the reasons we decided to go open source was so that other people could improve this project. If you find any bugs, please create a new issue or contact the lead developer on the project. If you have a fix, then please submit a patch. Make sure that you have added new test cases that show what the patch fixes.

# How do I use jPile?

jPile is very easy to use. If you are using Maven, then add the following dependency:

```xml
<dependency>
    <groupId>com.opower</groupId>
    <artifactId>jpile</artifactId>
    <version>1.7.11</version>
</dependency>
```

The most common use case is to create a new instance of `HierarchicalInfileObjectLoader`. You have to provide a valid database `Connection`. `HierarchicalInfileObjectLoader` doesn't rely on a database pool because it needs to disable foreign key constraints. Using multiple connections would fail because each new connection would have foreign key constraints enabled by default. Below shows how to do this.

```java
try (Connection connection = ...;
     HierarchicalInfileObjectLoader hierarchicalInfileObjectLoader = new HierarchicalInfileObjectLoader()
) {
    hierarchicalInfileObjectLoader.setConnection(connection);
    hierarchicalInfileObjectLoader.persist(myEntity);
    // Add more using persist()
}
```

In order to get events about loading process use `HierarchicalInfileObjectLoader.subscribe()` and otherwise `HierarchicalInfileObjectLoader.unsubscribe()` to stop receiving events.
Listener should have have public method that accepts appropriate event as argument and marked by `@Subscribe` annotation.
```java
public class Listener {
    @Subscribe public void handle(SaveEntityEvent event) { /* handler code for SaveEntityEvent events  */ }
    @Subscribe public void handle(FlushEvent event) { /* handler code for FlushEvent events */ }
}
```
Refer to [documentation](https://github.com/google/guava/wiki/EventBusExplained) to get more information.

# What license is jPile released under?

jPile is released on the MIT license which is available in `license.txt` to read.

# How was the performance comparison done?

By running the performance test: ```mvn clean install -Dperformance```

25,000 fake objects were created. Each object has a Customer, Contact (One-to-one) and 4 Products (One-to-many) which have a Supplier (Many-to-one). All these objects were saved using simple MySQL prepared statements, Hibernate, and jPile. The results were as follows:

* Prepared Statements - 60s
* Hibernate - 40s
* jPile - 6s

## Performance Graph

![Performance Graph](http://i.imgur.com/2yiT2.jpg)

# FindBugs

jPile uses the FindBugs tool to perform various static analysis checks.
FindBugs is run automatically when `mvn verify` is run.
You can run `mvn site` and look at the generated `findbugs.html` to list any bugs found.

FindBugs can also be configured in your IDE of choice.
Maven is configured to use the following settings:

- Analysis Effort: Maximal
- Minimum Confidence to Report: Medium
