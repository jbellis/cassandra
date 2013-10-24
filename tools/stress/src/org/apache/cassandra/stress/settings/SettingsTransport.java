package org.apache.cassandra.stress.settings;

import org.apache.thrift.transport.TTransportFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsTransport
{

//    availableOptions.addOption("ts", SSL_TRUSTSTORE,         true, "SSL: full path to truststore");
//    availableOptions.addOption("tspw", SSL_TRUSTSTORE_PW,    true, "SSL: full path to truststore");
//    availableOptions.addOption("prtcl", SSL_PROTOCOL,        true, "SSL: connections protocol to use (default: TLS)");
//    availableOptions.addOption("alg", SSL_ALGORITHM,         true, "SSL: algorithm (default: SunX509)");
//    availableOptions.addOption("st", SSL_STORE_TYPE,         true, "SSL: type of store");
//    availableOptions.addOption("ciphers", SSL_CIPHER_SUITES, true, "SSL: comma-separated list of encryption suites to use");
//    availableOptions.addOption("tf", "transport-factory",    true,   "Fully-qualified TTransportFactory class name for creating a connection. Note: For Thrift over SSL, use org.apache.cassandra.stress.SSLTransportFactory.");

//    private static final String SSL_TRUSTSTORE = "truststore";
//    private static final String SSL_TRUSTSTORE_PW = "truststore-password";
//    private static final String SSL_PROTOCOL = "ssl-protocol";
//    private static final String SSL_ALGORITHM = "ssl-alg";
//    private static final String SSL_STORE_TYPE = "store-type";
//    private static final String SSL_CIPHER_SUITES = "ssl-ciphers";

    public final TTransportFactory factory;

    static class TOptions extends GroupedOptions
    {
        final OptionSimple factory = new OptionSimple("factory=", ".*", "org.apache.cassandra.cli.transport.FramedTransportFactory", "Fully-qualified TTransportFactory class name for creating a connection. Note: For Thrift over SSL, use org.apache.cassandra.stress.SSLTransportFactory.", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(factory);
        }
    }

    static final class SSLOptions extends TOptions
    {
        final OptionSimple trustStore = new OptionSimple("truststore=", ".*", null, "SSL: full path to truststore", false);
        final OptionSimple trustStorePw = new OptionSimple("truststore-password=", ".*", null, "", false);
        final OptionSimple protocol = new OptionSimple("ssl-protocol=", ".*", "TLS", "SSL: connections protocol to use", false);
        final OptionSimple alg = new OptionSimple("ssl-alg=", ".*", "SunX509", "SSL: algorithm", false);
        final OptionSimple storeType = new OptionSimple("store-type=", ".*", "TLS", "SSL: comma delimited list of encryption suites to use", false);
        final OptionSimple ciphers = new OptionSimple("ssl-ciphers=", ".*", "TLS", "SSL: comma delimited list of encryption suites to use", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(factory, trustStore, trustStorePw, protocol, alg, storeType, ciphers);
        }
    }

    public SettingsTransport(TOptions options)
    {
        if (options instanceof SSLOptions)
        {
            throw new NotImplementedException();
        }
        else
        {
            try
            {
                this.factory = (TTransportFactory) Class.forName(options.factory.value()).newInstance();
            }
            catch (Exception e)
            {
                throw new IllegalArgumentException("Invalid transport factory class: " + options.factory.value(), e);
            }
        }
    }

    public static SettingsTransport get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-transport");
        if (params == null)
            return new SettingsTransport(new TOptions());

        GroupedOptions options = GroupedOptions.select(params, new TOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -transport options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsTransport((TOptions) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-transport", new TOptions());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }

}
