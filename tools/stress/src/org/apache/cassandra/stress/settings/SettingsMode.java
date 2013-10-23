package org.apache.cassandra.stress.settings;

public class SettingsMode
{

//    availableOptions.addOption("L",  "enable-cql",           false,  "Perform queries using CQL2 (Cassandra Query Language v 2.0.0)");
//    availableOptions.addOption("L3", "enable-cql3",          false,  "Perform queries using CQL3 (Cassandra Query Language v 3.0.0)");
//    availableOptions.addOption("b",  "enable-native-protocol",  false,  "Use the binary native protocol (only work along with -L3)");
//    availableOptions.addOption("P",  "use-prepared-statements", false, "Perform queries using prepared statements (only applicable to CQL).");

    public final ConnectionAPI api;
    public final CqlVersion cqlVersion;
    public final boolean nativeProtocol;
    public final boolean usePreparedStatements;

    private static final class Cql3Options
    {

    }

    public SettingsMode(ConnectionAPI api, CqlVersion cqlVersion, boolean nativeProtocol, boolean usePreparedStatements)
    {
        this.api = api;
        this.cqlVersion = cqlVersion;
        this.nativeProtocol = nativeProtocol;
        this.usePreparedStatements = usePreparedStatements;
    }
}
