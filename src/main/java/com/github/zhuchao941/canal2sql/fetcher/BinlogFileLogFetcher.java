package com.github.zhuchao941.canal2sql.fetcher;

import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.LogFetcher;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;

/**
 * TODO: Document It!!
 *
 * <pre>
 * FileLogFetcher fetcher = new FileLogFetcher();
 * fetcher.open(file, 0);
 *
 * while (fetcher.fetch()) {
 *     LogEvent event;
 *     do {
 *         event = decoder.decode(fetcher, context);
 *
 *         // process log event.
 *     } while (event != null);
 * }
 * // file ending reached.
 * </pre>
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class BinlogFileLogFetcher extends LogFetcher {

    public static final byte[] BINLOG_MAGIC = {-2, 0x62, 0x69, 0x6e};

    private InputStream in;

    public BinlogFileLogFetcher() {
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public BinlogFileLogFetcher(final int initialCapacity) {
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public BinlogFileLogFetcher(final int initialCapacity, final float growthFactor) {
        super(initialCapacity, growthFactor);
    }

    /**
     * Open binlog file in local disk to fetch.
     */
    public void open(String filePath) throws FileNotFoundException, IOException {
        open(filePath, 0L);
    }

    /**
     * Open binlog file in local disk to fetch.
     */
    public void open(String urlStr, final long filePosition) throws FileNotFoundException, IOException {

        URL url = new URL(urlStr);
        URLConnection urlConnection = url.openConnection();
        in = urlConnection.getInputStream();

        ensureCapacity(BIN_LOG_HEADER_SIZE);
        if (BIN_LOG_HEADER_SIZE != in.read(buffer, 0, BIN_LOG_HEADER_SIZE)) {
            throw new IOException("No binlog file header");
        }

        if (buffer[0] != BINLOG_MAGIC[0] || buffer[1] != BINLOG_MAGIC[1] || buffer[2] != BINLOG_MAGIC[2]
                || buffer[3] != BINLOG_MAGIC[3]) {
            throw new IOException("Error binlog file header: "
                    + Arrays.toString(Arrays.copyOf(buffer, BIN_LOG_HEADER_SIZE)));
        }

        limit = 0;
        origin = 0;
        position = 0;

        if (filePosition > BIN_LOG_HEADER_SIZE) {
            final int maxFormatDescriptionEventLen = FormatDescriptionLogEvent.LOG_EVENT_MINIMAL_HEADER_LEN
                    + FormatDescriptionLogEvent.ST_COMMON_HEADER_LEN_OFFSET
                    + LogEvent.ENUM_END_EVENT + LogEvent.BINLOG_CHECKSUM_ALG_DESC_LEN
                    + LogEvent.CHECKSUM_CRC32_SIGNATURE_LEN;

            ensureCapacity(maxFormatDescriptionEventLen);
            limit = in.read(buffer, 0, maxFormatDescriptionEventLen);
            limit = (int) getUint32(LogEvent.EVENT_LEN_OFFSET);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see LogFetcher#fetch()
     */
    public boolean fetch() throws IOException {
        if (in == null) {
            return false;
        }
        // 先判断当前buffer里的数据够不够一次事件
        if (limit < FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN) {
            // 如果不到一个头就先读一个头
            int length = readFully(in, buffer, origin + limit, FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN);
            if (length < 0) {
                return false;
            }
            limit += length;
        }
        long eventLen = getUint32(LogEvent.EVENT_LEN_OFFSET);
        if (limit >= eventLen) {
            // 足够一次完整事件，无需从底层读取数据
            return true;
        }
        // 先确保buffer足够
        ensureCapacity((int) eventLen);
        // 从底层读满一个事件
        int length = readFully(in, buffer, origin + limit, (int) (eventLen - FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN));
        if (length < 0) {
            return false;
        }
        limit += length;
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @see LogFetcher#close()
     */
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }

        in = null;
    }

    public int readFully(InputStream in, byte b[], int off, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);
            if (count < 0)
                return n == 0 ? -1 : n;
            n += count;
        }
        return n;
    }
}
