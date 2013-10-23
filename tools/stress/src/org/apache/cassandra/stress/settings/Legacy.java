package org.apache.cassandra.stress.settings;

import org.apache.commons.cli.*;

public class Legacy
{

    public static final Options availableOptions = new Options();

    private static final String SSL_TRUSTSTORE = "truststore";
    private static final String SSL_TRUSTSTORE_PW = "truststore-password";
    private static final String SSL_PROTOCOL = "ssl-protocol";
    private static final String SSL_ALGORITHM = "ssl-alg";
    private static final String SSL_STORE_TYPE = "store-type";
    private static final String SSL_CIPHER_SUITES = "ssl-ciphers";

    static
    {
        availableOptions.addOption("h",  "help",                 false,  "Show this help message and exit");
        availableOptions.addOption("n",  "num-keys",             true,   "Number of keys, default:1000000");
        availableOptions.addOption("F",  "num-different-keys",   true,   "Number of different keys (if < NUM-KEYS, the same key will re-used multiple times), default:NUM-KEYS");
        availableOptions.addOption("t",  "threads",              true,   "Number of threads to use, default:50");
        availableOptions.addOption("c",  "columns",              true,   "Number of columns per key, default:5");
        availableOptions.addOption("S",  "column-size",          true,   "Size of column values in bytes, default:34");
        availableOptions.addOption("C",  "unique columns",       true,   "Max number of unique columns per key, default:50");
        availableOptions.addOption("RC", "unique rows",          true,   "Max number of unique rows, default:50");
        availableOptions.addOption("d",  "nodes",                true,   "Host nodes (comma separated), default:locahost");
        availableOptions.addOption("D",  "nodesfile",            true,   "File containing host nodes (one per line)");
        availableOptions.addOption("s",  "stdev",                true,   "Standard Deviation for gaussian read key generation, default:0.1");
        availableOptions.addOption("r",  "random",               false,  "Use random key generator for read key generation (STDEV will have no effect), default:false");
        availableOptions.addOption("f",  "file",                 true,   "Write output to given file");
        availableOptions.addOption("p",  "port",                 true,   "Thrift port, default:9160");
        availableOptions.addOption("o",  "operation",            true,   "Operation to perform (INSERT, READ, READWRITE, RANGE_SLICE, INDEXED_RANGE_SLICE, MULTI_GET, COUNTER_ADD, COUNTER_GET), default:INSERT");
        availableOptions.addOption("u",  "supercolumns",         true,   "Number of super columns per key, default:1");
        availableOptions.addOption("y",  "family-type",          true,   "Column Family Type (Super, Standard), default:Standard");
        availableOptions.addOption("K",  "keep-trying",          true,   "Retry on-going operation N times (in case of failure). positive integer, default:10");
        availableOptions.addOption("k",  "keep-going",           false,  "Ignore errors inserting or reading (when set, --keep-trying has no effect), default:false");
        availableOptions.addOption("i",  "progress-interval",    true,   "Progress Report Interval (seconds), default:10");
        availableOptions.addOption("g",  "keys-per-call",        true,   "Number of keys to get_range_slices or multiget per call, default:1000");
        availableOptions.addOption("l",  "replication-factor",   true,   "Replication Factor to use when creating needed column families, default:1");
        availableOptions.addOption("L",  "enable-cql",           false,  "Perform queries using CQL2 (Cassandra Query Language v 2.0.0)");
        availableOptions.addOption("L3", "enable-cql3",          false,  "Perform queries using CQL3 (Cassandra Query Language v 3.0.0)");
        availableOptions.addOption("b",  "enable-native-protocol",  false,  "Use the binary native protocol (only work along with -L3)");
        availableOptions.addOption("P",  "use-prepared-statements", false, "Perform queries using prepared statements (only applicable to CQL).");
        availableOptions.addOption("e",  "consistency-level",    true,   "Consistency Level to use (ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY), default:ONE");
        availableOptions.addOption("x",  "create-index",         true,   "Type of index to create on needed column families (KEYS)");
        availableOptions.addOption("R",  "replication-strategy", true,   "Replication strategy to use (only on insert if keyspace does not exist), default:org.apache.cassandra.locator.SimpleStrategy");
        availableOptions.addOption("O",  "strategy-properties",  true,   "Replication strategy properties in the following format <dc_name>:<num>,<dc_name>:<num>,...");
        availableOptions.addOption("W",  "no-replicate-on-write",false,  "Set replicate_on_write to false for counters. Only counter add with CL=ONE will work");
        availableOptions.addOption("V",  "average-size-values",  false,  "Generate column values of average rather than specific size");
        availableOptions.addOption("T",  "send-to",              true,   "Send this as a request to the stress daemon at specified address.");
        availableOptions.addOption("I",  "compression",          true,   "Specify the compression to use for sstable, default:no compression");
        availableOptions.addOption("Q",  "query-names",          true,   "Comma-separated list of column names to retrieve from each row.");
        availableOptions.addOption("Z",  "compaction-strategy",  true,   "CompactionStrategy to use.");
        availableOptions.addOption("U",  "comparator",           true,   "Column Comparator to use. Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");
        availableOptions.addOption("tf", "transport-factory",    true,   "Fully-qualified TTransportFactory class name for creating a connection. Note: For Thrift over SSL, use org.apache.cassandra.stress.SSLTransportFactory.");
        availableOptions.addOption("ns", "no-statistics",        false,  "Turn off the aggegate statistics that is normally output after completion.");
        availableOptions.addOption("ts", SSL_TRUSTSTORE,         true, "SSL: full path to truststore");
        availableOptions.addOption("tspw", SSL_TRUSTSTORE_PW,    true, "SSL: full path to truststore");
        availableOptions.addOption("prtcl", SSL_PROTOCOL,        true, "SSL: connections protocol to use (default: TLS)");
        availableOptions.addOption("alg", SSL_ALGORITHM,         true, "SSL: algorithm (default: SunX509)");
        availableOptions.addOption("st", SSL_STORE_TYPE,         true, "SSL: type of store");
        availableOptions.addOption("ciphers", SSL_CIPHER_SUITES, true, "SSL: comma-separated list of encryption suites to use");
        availableOptions.addOption("th",  "throttle",            true,   "Throttle the total number of operations per second to a maximum amount.");
    }

}
