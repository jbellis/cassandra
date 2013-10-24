package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

public class SettingsMisc implements Serializable
{

    public static Runnable helpHelpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println("Usage: ./bin/cassandra-stress help <command|option>");
                System.out.println("Commands:");
                for (Command cmd : Command.values())
                    System.out.println("    " + cmd.toString().toLowerCase() + (cmd.extraName != null ? ", " + cmd.extraName : ""));
                System.out.println("Options:");
                for (CliOption op : CliOption.values())
                    System.out.println("    -" + op.toString().toLowerCase() + (op.extraName != null ? ", " + op.extraName : ""));
            }
        };
    }

    public static Runnable portHelpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println("Usage: -port <port>");
                System.out.println();
                System.out.println("Specify the port of the cassandra server to connect to (default is 9160)");
            }
        };
    }

    public static Runnable sendToDaemonHelpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println("Usage: -sendToDaemon <host>");
                System.out.println();
                System.out.println("Specify a host running the stress server to send this stress command to");
            }
        };
    }

    public static int getPort(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-port");
        if (params == null)
            return 9160;
        if (params.length != 1)
        {
            portHelpPrinter().run();
            System.out.println("Invalid -port specifier: " + Arrays.toString(params));
            System.exit(1);
        }
        return Integer.parseInt(params[0]);

    }

    public static String getSendToDaemon(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-send-to");
        if (params == null)
            params = clArgs.remove("-sendto");
        if (params == null)
            return null;
        if (params.length != 1)
        {
            sendToDaemonHelpPrinter().run();
            System.out.println("Invalid -send-to specifier: " + Arrays.toString(params));
            System.exit(1);
        }
        return params[0];

    }

}
