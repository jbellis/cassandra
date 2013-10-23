package org.apache.cassandra.stress.settings;

import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.generatedata.DistributionBoundApache;
import org.apache.cassandra.stress.generatedata.DistributionFactory;
import org.apache.cassandra.stress.generatedata.DistributionFixed;
import org.apache.commons.math3.distribution.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OptionDistribution implements Option
{

    private static final Pattern FULL = Pattern.compile("([A-Z]+)\\((.+)\\)");
    private static final Pattern ARGS = Pattern.compile("[^,]+");

    final String prefix;
    private DistributionFactory factory;
    private final DistributionFactory defaultFactory;

    public OptionDistribution(String prefix, String defaultSpec)
    {
        this.prefix = prefix;
        this.defaultFactory = defaultSpec == null ? null : get(defaultSpec);
    }

    @Override
    public boolean accept(String param)
    {
        if (!param.toLowerCase().startsWith(prefix))
            return false;
        factory = get(param.substring(prefix.length()));
        return true;
    }

    private static DistributionFactory get(String spec)
    {
        Matcher m = FULL.matcher(spec);
        if (!m.matches())
            throw new IllegalArgumentException("Illegal distribution specification: " + spec);
        String name = m.group(1);
        Impl impl = LOOKUP.get(name.toLowerCase());
        if (impl == null)
            throw new IllegalArgumentException("Illegal distribution type: " + name);
        List<String> params = new ArrayList<>();
        m = ARGS.matcher(m.group(2));
        while (m.find())
            params.add(m.group());
        return impl.getFactory(params);
    }

    public DistributionFactory get()
    {
        return factory != null ? factory : defaultFactory;
    }

    @Override
    public boolean happy()
    {
        return factory != null || defaultFactory != null;
    }

    @Override
    public String description()
    {
        return "Specify a mathematical distribution";
    }

    public String toString()
    {
        return "GAUSSIAN(<minKey>..<maxKey>,<stdevs from mean to minKey/maxKey>), UNIFORM(minKey..maxKey), ";
    }

    private static final Map<String, Impl> LOOKUP;
    static
    {
        final Map<String, Impl> lookup = new HashMap<>();
        lookup.put("gaussian", new GaussianImpl());
        lookup.put("normal", new GaussianImpl());
        lookup.put("gauss", new GaussianImpl());
        lookup.put("norm", new GaussianImpl());
        lookup.put("uniform", new UniformImpl());
        LOOKUP = lookup;
    }

    private static interface Impl
    {
        public DistributionFactory getFactory(List<String> params);
    }

    private static final class GaussianImpl implements Impl
    {
        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() > 3 || params.size() < 1)
                throw new IllegalArgumentException("Invalid parameter list for gaussian distribution: " + params);
            try
            {
                String[] bounds = params.get(0).split("\\.\\.+");
                final long minKey = Long.parseLong(bounds[0]);
                final long maxKey = Long.parseLong(bounds[1]);
                final double stdevsToEdge = params.size() == 1 ? 3d : Double.parseDouble(params.get(1));
                final double mean = (minKey + maxKey) / 2d;
                final double stdev = ((maxKey - minKey) / 2d) / stdevsToEdge;
                return new DistributionFactory()
                {
                    @Override
                    public Distribution get()
                    {
                        return new DistributionBoundApache(new NormalDistribution(mean, stdev), minKey, maxKey);
                    }
                };
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            }
        }
    }

    private static final class UniformImpl implements Impl
    {

        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() != 1)
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            try
            {
                String[] bounds = params.get(0).split("\\.\\.+");
                final long minKey = Long.parseLong(bounds[0]);
                final long maxKey = Long.parseLong(bounds[1]);
                return new DistributionFactory()
                {
                    @Override
                    public Distribution get()
                    {
                        return new DistributionBoundApache(new UniformRealDistribution(minKey, maxKey), minKey, maxKey);
                    }
                };
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            }
        }
    }

    private static final class FixedImpl implements Impl
    {

        @Override
        public DistributionFactory getFactory(List<String> params)
        {
            if (params.size() != 1)
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            try
            {
                final long key = Long.parseLong(params.get(0));
                return new DistributionFactory()
                {
                    @Override
                    public Distribution get()
                    {
                        return new DistributionFixed(key);
                    }
                };
            } catch (Exception _)
            {
                throw new IllegalArgumentException("Invalid parameter list for uniform distribution: " + params);
            }
        }
    }

}
