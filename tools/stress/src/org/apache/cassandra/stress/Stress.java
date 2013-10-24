/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stress;

import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.commons.cli.Option;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;

public final class Stress
{

    private static volatile boolean stopped = false;

    public static void main(String[] arguments) throws Exception
    {
        final StressSettings settings;
        try
        {
            settings = StressSettings.parse(arguments);
        }
        catch (IllegalArgumentException e)
        {
            printHelpMessage();
            e.printStackTrace();
            return;
        }

        PrintStream logout = settings.log.getOutput();

        if (settings.sendToDaemon != null)
        {
            Socket socket = new Socket(settings.sendToDaemon, 2159);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            BufferedReader inp = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            Runtime.getRuntime().addShutdownHook(new ShutDown(socket, out));

            out.writeObject(settings);

            String line;

            try
            {
                while (!socket.isClosed() && (line = inp.readLine()) != null)
                {
                    if (line.equals("END") || line.equals("FAILURE"))
                    {
                        out.writeInt(1);
                        break;
                    }

                    logout.println(line);
                }
            }
            catch (SocketException e)
            {
                if (!stopped)
                    e.printStackTrace();
            }

            out.close();
            inp.close();

            socket.close();
        }
        else
        {
            StressAction stressAction = new StressAction(settings, logout);
            stressAction.run();
        }
    }

    /**
     * Printing out help message
     */
    public static void printHelpMessage()
    {
        StressSettings.printHelp();
    }

    private static class ShutDown extends Thread
    {
        private final Socket socket;
        private final ObjectOutputStream out;

        public ShutDown(Socket socket, ObjectOutputStream out)
        {
            this.out = out;
            this.socket = socket;
        }

        public void run()
        {
            try
            {
                if (!socket.isClosed())
                {
                    System.out.println("Control-C caught. Canceling running action and shutting down...");

                    out.writeInt(1);
                    out.close();

                    stopped = true;
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

}
