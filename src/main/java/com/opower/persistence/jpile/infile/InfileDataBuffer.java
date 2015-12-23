package com.opower.persistence.jpile.infile;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.opower.persistence.jpile.reflection.CachedProxy;
import com.opower.persistence.jpile.reflection.PersistenceAnnotationInspector;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.persistence.Temporal;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Date;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A buffer used to collect data in MySQL's infile format. This buffer also maintains a separate row buffer
 * and implements methods to allow clients to clear and append various data types to said row. These methods insert
 * field and line separators as needed as well as provide proper formats for declaring date and null values.
 * <p/>
 * When the current row is complete, it can be added to the infile buffer via {@link #addRowToInfile()}. If the row
 * does not fit into the infile buffer, none of its contents are added. To make room, clients should read the contents
 * of the infile buffer with {@link #asInputStream()} and then clear it. In general,clients should consider implementing
 * an {@link com.opower.persistence.jpile.loader.InfileObjectLoader} to manage infile buffers. That class provides higher
 * level interaction and
 * management of these buffers.
 * <p/>
 * Instances of this class are not safe for use by multiple threads.
 *
 * @author Sean-Michael
 * @author aaron.silverman
 * @author amir.raminfar
 * @see <a href="http://dev.mysql.com/doc/refman/5.1/en/load-data.html">LOAD DATA INFILE reference</a>
 * @since 1.0
 */
public class InfileDataBuffer implements InfileRow {
    /**
     * Default size in bytes of the infile buffer.
     */
    public static final int DEFAULT_INFILE_BUFFER_SIZE = 10 * 1024 * 1024; // 10MB
    /**
     * Default size in bytes of the row buffer.
     */
    public static final int DEFAULT_ROW_BUFFER_SIZE = 1024 * 10; // 10kB

    // Infile constants
    protected static final char MYSQL_ESCAPE_CHAR = '\\';
    protected static final String MYSQL_NULL_STRING = MYSQL_ESCAPE_CHAR + "N";
    // List of bytes that will need escaping as they hold special meaning to MYSQL
    // See http://dev.mysql.com/doc/refman/5.1/en/load-data.html
    protected static final Set<Byte> BYTES_NEEDING_ESCAPING = ImmutableSet.of(
            (byte) '\0',
            (byte) '\b',
            (byte) '\n',
            (byte) '\r',
            (byte) '\t',
            (byte) MYSQL_ESCAPE_CHAR,
            (byte) 26);
    private static final String TEMPORAL_TYPE_EXCEPTION =
            "The Temporal.value should be TemporalType.DATE, TemporalType.TIME, or TemporalType.TIMESTAMP on method [%s]";
    // This Pattern matches on all of the BYTES_NEEDING_ESCAPING
    private static final Pattern ESCAPE_PATTERN = Pattern.compile("[\b\n\r\t\f\0\u001A\\\\]");

    // Using Joda time which is thread safe
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormat.forPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    // Utilities
    private final CharsetEncoder encoder;

    // Common byte sequences
    private final byte[] nullBytes;
    private final byte[] tabBytes;
    private final byte[] newlineBytes;

    // Buffers
    private final ByteBuffer infileBuffer;
    private final ByteBuffer rowBuffer;

    private PersistenceAnnotationInspector persistenceAnnotationInspector =
            CachedProxy.create(new PersistenceAnnotationInspector());

    public InfileDataBuffer(Charset charset, int infileBufferSize, int rowBufferSize) {
        Preconditions.checkNotNull(charset, "No charset set for encoding.");
        this.encoder = charset.newEncoder();

        // This not using the encoder because that API is tedious just to encode a few strings.
        this.tabBytes = "\t".getBytes(charset);
        this.newlineBytes = "\n".getBytes(charset);
        this.nullBytes = MYSQL_NULL_STRING.getBytes(charset);

        // Make sure the row buffer is not larger than the infile buffer. If that were allowed you'd get into cases
        // where you would not be able to write rows to the infile buffer even if it were empty.
        Preconditions.checkState(rowBufferSize <= infileBufferSize,
                                 "Cannot create a row buffer larger than the infile buffer.");

        this.rowBuffer = ByteBuffer.allocate(rowBufferSize);
        this.infileBuffer = ByteBuffer.allocate(infileBufferSize);
    }

    public InfileDataBuffer(Charset charset) {
        this(charset, DEFAULT_INFILE_BUFFER_SIZE, DEFAULT_ROW_BUFFER_SIZE);
    }

    public InfileDataBuffer() {
        this(Charsets.UTF_8);
    }

    /**
     * Attempts to add the current row to the infile buffer. If there is insufficient room for the current row
     * and -- if there is any other data in the buffer -- a newline, then the row is not added and the method returns
     * {@code false}.
     *
     * @return {@code true} if the current row fits into the infile (and has been added)
     */
    public boolean addRowToInfile() {
        boolean addNewline = this.infileBuffer.position() > 0;
        if (this.infileBuffer.remaining() < (this.rowBuffer.position() + (addNewline ? this.newlineBytes.length : 0))) {
            return false;
        }

        this.rowBuffer.flip();
        if (isRowBufferEmpty()) {
            return true;
        }

        if (addNewline) {
            this.infileBuffer.put(this.newlineBytes);
        }
        this.infileBuffer.put(this.rowBuffer);
        return true;
    }

    /**
     * @return true if the rowBuffer is empty.
     */
    private boolean isRowBufferEmpty() {
        return this.rowBuffer.position() == this.rowBuffer.limit();
    }

    /**
     * Gets a view of the contents of the infile buffer as input stream. Once you are done reading, you <i>must</i>
     * clear or reset this buffer.
     *
     * @return buffer contents
     */
    // CR MB: Do we want to add status flags to this class to prevent undefined use?
    public InputStream asInputStream() {
        this.infileBuffer.flip();
        return new ByteArrayInputStream(this.infileBuffer.array(), 0, this.infileBuffer.limit());
    }

    /**
     * Resets this buffer, clearing both the current row and the infile buffer.
     */
    public void reset() {
        this.infileBuffer.clear();
        this.rowBuffer.clear();
    }

    /**
     * Clears the contents of the infile buffer, but maintains the state of the current row.
     */
    public void clear() {
        this.infileBuffer.clear();
    }

    /**
     * Appends an encoded tab ('\t') character if current row has any data in it. Otherwise, it does nothing.
     */
    private void appendTabIfNeeded() {
        if (this.rowBuffer.position() > 0) {
            this.rowBuffer.put(this.tabBytes);
        }
    }

    @Override
    public final InfileRow append(byte b) {
        this.appendTabIfNeeded();
        appendByte(b);
        return this;
    }

    @Override
    public final InfileRow append(byte[] bytes) {
        this.appendTabIfNeeded();
        for (byte b : bytes) {
            appendByte(b);
        }
        return this;
    }

    @Override
    public InfileRow append(Float number, int precision, int scale) {
        checkArgument(scale > 0, "Scale (%s) should be greater than 0", scale);
        checkArgument(precision > 0, "Precision (%s) should be greater than 0", precision);
        checkArgument(scale < precision, "Scale (%s) must be no larger than precision (%s)", scale, precision);

        String integer = Strings.repeat("#", precision - scale);
        String fractional = Strings.repeat("#", scale);

        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
        decimalFormatSymbols.setDecimalSeparator('.');
        DecimalFormat decimalFormat = new DecimalFormat(integer + "." + fractional);
        decimalFormat.setGroupingUsed(false);
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);

        String formatted = decimalFormat.format(number);
        return append(formatted);
    }

    private void appendByte(byte b) {
        if (BYTES_NEEDING_ESCAPING.contains(b)) {
            this.rowBuffer.put((byte) MYSQL_ESCAPE_CHAR);
        }
        this.rowBuffer.put(b);
    }

    @Override
    public final InfileRow append(String s) {
        if (s == null) {
            return this.appendNull();
        }
        this.appendTabIfNeeded();
        String escapedStr = ESCAPE_PATTERN.matcher(s).replaceAll("\\\\$0");

        CoderResult result = this.encoder.encode(CharBuffer.wrap(escapedStr), this.rowBuffer, false);
        if (!result.isUnderflow()) {
            try {
                result.throwException();
            }
            catch (CharacterCodingException e) {
                throw new Error(e);
            }
        }
        return this;
    }

    @Override
    public final InfileRow append(Date d, Method method) {
        Temporal temporal = this.persistenceAnnotationInspector.findAnnotation(method, Temporal.class);
        Preconditions.checkNotNull(temporal, "A temporal annotation must be provided on method [%s]", method);

        switch (temporal.value()) {
            case DATE:
                return appendDate(d, DATE_FORMATTER);
            case TIME:
                return appendDate(d, TIME_FORMATTER);
            case TIMESTAMP:
                return appendDate(d, TIMESTAMP_FORMATTER);
            default:
                throw new IllegalArgumentException(String.format(TEMPORAL_TYPE_EXCEPTION, method));
        }
    }

    private InfileRow appendDate(Date d, DateTimeFormatter dateTimeFormatter) {
        return (d == null) ? this.appendNull() : this.append(dateTimeFormatter.print(d.getTime()));
    }

    @Override
    public final InfileRow append(Boolean b) {
        return (b == null) ? this.appendNull() : this.append(b ? 1 : 0);
    }

    @Override
    public final InfileRow append(Object o) {
        return (o == null) ? this.appendNull() : this.append(o.toString());
    }

    @Override
    public final InfileRow appendNull() {
        this.appendTabIfNeeded();
        this.rowBuffer.put(this.nullBytes);
        return this;
    }

    @Override
    public final InfileRow appendEscaped(String s) {
        return append(s.replace('\t', ','));
    }

    /**
     * Clears the current row and returns this buffer as row view.
     *
     * @return this
     */
    @Override
    public final InfileRow newRow() {
        this.rowBuffer.clear();
        return this;
    }
}
