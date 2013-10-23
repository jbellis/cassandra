package org.apache.cassandra.stress.settings;

public interface Option
{

    boolean accept(String param);
    boolean happy();
    String description();

}
