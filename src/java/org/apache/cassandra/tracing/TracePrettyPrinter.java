/*
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
package org.apache.cassandra.tracing;

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.*;
import com.google.common.primitives.Longs;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;


public class TracePrettyPrinter
{

    private static final double PRETTY_PRINT_CHAR_SCALE = 100.0;

    public static void printSingleSessionTrace(UUID sessionId, List<TraceEvent> events, PrintStream out)
    {
        TraceEvent first = Iterables.get(events, 0);
        TraceEvent last = null;
        Integer clValue = first.getFromPayload("consistency_level");
        ConsistencyLevel cl = null;
        if (clValue != null)
        {
            int consistencyLevel = (Integer)first.getFromPayload("consistency_level");
            cl = ConsistencyLevel.findByValue(consistencyLevel);
        }
        InetAddress coordinator = first.coordinator();
        String eventName = first.name();

        Multimap<InetAddress, TraceEvent> eventsPerNode = Multimaps.newListMultimap(
                Maps.<InetAddress, Collection<TraceEvent>> newLinkedHashMap(),
                new Supplier<ArrayList<TraceEvent>>()
                {
                    @Override
                    public ArrayList<TraceEvent> get()
                    {
                        return Lists.newArrayList();
                    }
                });

        Map<InetAddress, Collection<TraceEvent>> eventsPerNodeAsMap = eventsPerNode.asMap();

        for (TraceEvent event : events)
        {
            last = event;
            eventsPerNode.put(event.source(), event);
        }
        long sessionDuration = last.duration();
        // work in nanos to try and loose the least precision possible
        long timeOffset = first.timestamp() * 1000000;

        out.println();
        out.println("Session Summary: " + sessionId);
        out.println("Request: " + eventName);
        out.println("Coordinator: " + coordinator);
        out.println("Interacting nodes: " + eventsPerNodeAsMap.size() + " {" + eventsPerNodeAsMap.keySet() + "}");
        out.println("Duration: " + sessionDuration + " nano seconds (" + nanosToMillis(sessionDuration) + " msecs)");

        if (cl != null)
        {
            out.println("Consistency Level: " + cl.name());
        }

        out.println("Request Timeline:\n");
        SortedSet<Timeline> timelines = Sets.newTreeSet();
        for (Map.Entry<InetAddress, Collection<TraceEvent>> entries : eventsPerNodeAsMap.entrySet())
        {
            List<TraceEvent> nodeEvents = (List<TraceEvent>) entries.getValue();
            TraceEvent firstNodeEvent = nodeEvents.get(0);
            TraceEvent lastNodeEvent = nodeEvents.get(nodeEvents.size() - 1);
            long nodeOffset = firstNodeEvent.timestamp() * 1000000 - timeOffset;
            long nodeStartInTimeline = scaleToTimeline(nodeOffset, sessionDuration);
            long nodeFinishInTimeline = scaleToTimeline(nodeOffset + lastNodeEvent.duration(), sessionDuration);
            SortedMap<Long, TraceEvent> eventsInTimeline = Maps.newTreeMap();
            for (TraceEvent event : nodeEvents)
            {
                long eventPosition = scaleToTimeline(nodeOffset + event.duration(), sessionDuration);
                if (eventsInTimeline.containsKey(eventPosition))
                {
                    TraceEvent overlappedEvent = eventsInTimeline.get(eventPosition);
                    if (!(overlappedEvent instanceof CompoundEvent))
                        eventsInTimeline.put(eventPosition, new CompoundEvent().add(overlappedEvent).add(event));
                    else
                        ((CompoundEvent) overlappedEvent).add(event);
                }
                else
                    eventsInTimeline.put(eventPosition, event);
            }
            timelines.add(new Timeline(nodeStartInTimeline, nodeFinishInTimeline, lastNodeEvent.duration(), entries
                    .getKey(), first.coordinator(), eventsInTimeline));
        }
        for (Timeline timeline : timelines)
        {
            out.println(timeline.toString());
        }

        out.println();
        out.println("Caption: ");
        out.println(String.format("%-15s", "Se") + " - Session start (start of request: " + eventName + ")");
        out.println(String.format("%-15s", "/Se") + " - Session end (end of request: " + eventName + ")");
        out.println(String.format("%-15s", "St[StageName]") + " - Begin of Stage with StageName (e.g. Mutation)");
        out.println(String.format("%-15s", "/St[StageName]") + " - End of Stage with StageName");
        out.println(String.format("%-15s", "Msg") + " - Arrival of message from coordinator");
        out.println(String.format("%-15s", "/Msg") + " - Reply to message from coordinator");
        out.println(String.format("%-15s", "???") + " - Custom traces ");
        out.println();
        out.println("NOTE: timelines are drawn to scale but precision is limited to "
                + Math.round(PRETTY_PRINT_CHAR_SCALE)
                + " positions.\n "
                + "When multiple events occur in the same position (due to lack of precision) they are separated by ','\n"
                + " When two positions are too close together trace names are separated by ';'");
        out.println();
        out.println("Full event list (in order of occurrence)");
        out.println();
        for (TraceEvent event : events)
        {
            out.println(printEventLine(event));
        }
        out.println();
    }

    private static String printEventLine(TraceEvent event)
    {
        String format = "%-22s";
        return new StringBuilder().append("Id: ").append(event.id()).append(" Name: ")
                .append(String.format(format, event.name())).append(" Type: ")
                .append(String.format(format, event.type())).append(" Source: ")
                .append(String.format(format, event.source()))
                .append(" Timestamp (millis): ").append(String.format(format, event.timestamp()))
                .append(" Duration (nanos): ")
                .append(String.format(format, event.duration())).toString();
    }

    private static long scaleToTimeline(double value, double totalDuration)
    {
        return Math.round((value * PRETTY_PRINT_CHAR_SCALE) / totalDuration);
    }

    private static String shortNames(TraceEvent event)
    {

        switch (event.type())
        {
        case SESSION_START:
            return "Se";
        case SESSION_END:
            return "/Se";
        case STAGE_START:
            return "St:[" + event.name().replace("Stage", "") + "]";
        case STAGE_FINISH:
            return "/St:[" + event.name().replace("Stage", "") + "]";
        case MESSAGE_ARRIVAL:
            return "/Msg";
        case MESSAGE_DEPARTURE:
            return "Msg";
        case MISC:
            return event.name();
        default:
            throw new IllegalStateException();
        }

    }

    private static final String LINE =
            "|----------------------------------------------------------------------------" +
                    "-----------------------|";

    public static void printMultiSessionTraceForRequestType(String requestName, Map<UUID, List<TraceEvent>> sessions,
            PrintStream out)
    {

        DescriptiveStatistics latencySstats = new DescriptiveStatistics();
        int totalEvents = 0;
        UUID minSessionId = null;
        UUID maxSessionId = null;
        double min = Long.MAX_VALUE;
        double max = 0;
        for (List<TraceEvent> events : sessions.values())
        {
            TraceEvent first = events.get(0);
            TraceEvent last = events.get(events.size() - 1);
            double duration = nanosToMillis(last.duration());
            if (duration < min)
            {
                minSessionId = first.sessionId();
                min = duration;
            }
            if (duration > max)
            {
                maxSessionId = first.sessionId();
                max = duration;
            }
            latencySstats.addValue(duration);
            totalEvents += events.size();

        }

        out.println("Summary for sessions of request: " + requestName);
        out.println("Total Sessions: " + sessions.values().size());
        out.println("Total Events: " + totalEvents);
        out.println("                       ==============================================================================");
        out.println("                       |    Avg.    |   StdDev.  |   Max.     |    Min.    |     99%   |     Unit   |");
        out.println("=====================================================================================================");
        printStatsLine(out, "Latency", latencySstats, "msec");
        printRequestSpecificInfo(requestName, sessions, out);
        out.println();
        out.println("Quickest Request sessionId: " + minSessionId);
        out.println("Slowest  Request sessionId: " + maxSessionId);
        out.println();
    }

    private static void printRequestSpecificInfo(String requestName, Map<UUID, List<TraceEvent>> traceEvents,
            PrintStream out)
    {
        if (requestName.equals("batch_mutate"))
            printBatchMutateInfo(traceEvents, out);
    }

    private static void printStatsLine(PrintStream out, String name, DescriptiveStatistics stats, String unit)
    {
        out.println("| " + formatString(name) + " | " + formatNumber(stats.getMean()) + " | "
                + formatNumber(stats.getStandardDeviation()) + " | " + formatNumber(stats.getMax()) + " | "
                + formatNumber(stats.getMin()) + " | " + formatNumber(stats.getPercentile(99.0)) + "| "
                + String.format("%10s", unit) + " |");
        out.println(LINE);
    }

    private static void printBatchMutateInfo(Map<UUID, List<TraceEvent>> traceEvents, PrintStream out)
    {
        final DescriptiveStatistics keysStats = new DescriptiveStatistics();
        final DescriptiveStatistics mutationStats = new DescriptiveStatistics();
        final DescriptiveStatistics deletionStats = new DescriptiveStatistics();
        final DescriptiveStatistics columnStats = new DescriptiveStatistics();
        final DescriptiveStatistics counterStats = new DescriptiveStatistics();
        final DescriptiveStatistics superColumnStats = new DescriptiveStatistics();
        final DescriptiveStatistics writtenSizeStats = new DescriptiveStatistics();
        applyToAll(traceEvents, new Function<TraceEvent, Void>()
        {

            @Override
            public Void apply(TraceEvent event)
            {
                if (event.type() == TraceEvent.Type.SESSION_START)
                {
                    keysStats.addValue((Integer) event.getFromPayload("total_keys"));
                    mutationStats.addValue((Integer) event.getFromPayload("total_mutations"));
                    deletionStats.addValue((Integer) event.getFromPayload("total_deletions"));
                    columnStats.addValue((Integer) event.getFromPayload("total_columns"));
                    counterStats.addValue((Integer) event.getFromPayload("total_counters"));
                    superColumnStats.addValue((Integer) event.getFromPayload("total_super_columns"));
                    writtenSizeStats.addValue((Long) event.getFromPayload("total_written_size"));
                }
                return null;
            }
        });
        printStatsLine(out, "Batch Rows", keysStats, "amount/req");
        printStatsLine(out, "Mutations", mutationStats, "amount/req");
        printStatsLine(out, "Deletions", deletionStats, "amount/req");
        printStatsLine(out, "Columns", columnStats, "amount/req");
        printStatsLine(out, "Counters", counterStats, "amount/req");
        printStatsLine(out, "Super Col.", superColumnStats, "amount/req");
        printStatsLine(out, "Written Bytes", writtenSizeStats, "amount/req");
    }

    private static String formatNumber(double number)
    {
        return String.format("%10.2f", number);
    }

    private static String formatString(String name)
    {
        return String.format("%-20s", name);
    }

    private static double nanosToMillis(long nanos)
    {
        // don't use TimeUnit.convert() because it truncates and we don't want to accumulate errors
        return nanos / 1000000.0;
    }

    private static void applyToAll(Map<UUID, List<TraceEvent>> traceEvents, Function<TraceEvent, ?> function)
    {
        Iterable<TraceEvent> all = Iterables.concat(traceEvents.values());
        for (TraceEvent event : all)
        {
            function.apply(event);
        }
    }

    // builds a timeline for a node
    private static class Timeline implements Comparable<Timeline>
    {

        private final long start;
        private final long finish;
        private final InetAddress source;
        private final long duration;
        private final SortedMap<Long, TraceEvent> eventsInTimeline;

        public Timeline(long start, long finish, long duration, InetAddress source, InetAddress coordinator,
                SortedMap<Long, TraceEvent> eventsInTimeline)
        {
            this.start = start;
            this.finish = finish;
            this.source = source;
            this.duration = duration;
            this.eventsInTimeline = eventsInTimeline;
        }

        public String toString()
        {
            return new StringBuilder().append(buildTimeline()).append("\n")
                    .append(buildEventline()).append("\n").toString();
        }

        private StringBuilder buildTimeline()
        {
            StringBuilder builder = new StringBuilder();
            long pos = 0;
            for (; pos < start; pos++)
                builder.append(' ');

            long lastPos = eventsInTimeline.lastKey();
            for (; pos <= lastPos; pos++)
            {
                if (eventsInTimeline.containsKey(pos))
                    builder.append('|');
                else
                    builder.append('-');
            }
            for (; pos <= PRETTY_PRINT_CHAR_SCALE; pos++)
                builder.append(' ');

            return builder;
        }

        private StringBuilder buildEventline()
        {
            StringBuilder builder = new StringBuilder();
            long pos = 0;
            for (; pos < start; pos++)
                builder.append(' ');

            long lastPos = eventsInTimeline.lastKey();
            long overoccupancy = 0;
            while (pos <= lastPos)
            {
                if (eventsInTimeline.containsKey(pos))
                {
                    String eventDesc = shortNames(eventsInTimeline.get(pos));
                    if (builder.length() > 0 && builder.charAt(builder.length() - 1) != ' ')
                    {
                        builder.append(";");
                    }
                    builder.append(eventDesc);
                    overoccupancy = overoccupancy > 0 ? overoccupancy + eventDesc.length() : eventDesc.length() - 1;
                }
                else
                {
                    if (overoccupancy > 0)
                        overoccupancy--;
                    else
                        builder.append(' ');
                }

                pos++;
            }
            for (; pos <= PRETTY_PRINT_CHAR_SCALE; pos++)
                builder.append(' ');
            return builder;
        }

        @Override
        public int compareTo(Timeline o)
        {
            return Longs.compare(start, o.start);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Timeline timeline = (Timeline) o;

            if (source != null ? !source.equals(timeline.source) : timeline.source != null)
                return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            return source != null ? source.hashCode() : 0;
        }
    }

    // helper for when multiple events fall in the same position
    private static class CompoundEvent extends TraceEvent
    {

        private List<TraceEvent> events = Lists.newArrayList();

        CompoundEvent()
        {
            super(null, null, 0, 0, null, null, null, null, null, null, null);
        }

        public CompoundEvent add(TraceEvent event)
        {
            this.events.add(event);
            return this;
        }

        public Type type()
        {
            return Type.MISC;
        }

        public String name()
        {
            return Joiner.on(",").join(Iterables.transform(events, new Function<TraceEvent, String>()
            {
                @Override
                public String apply(TraceEvent traceEvent)
                {
                    return shortNames(traceEvent);
                }
            }));
        }

    }
}
