package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class CommitLogArchiver
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogArchiver.class);
    public final List<String> archivePending = Collections.synchronizedList(new ArrayList<String>());
    private final String archiveCommand;
    private final String recoveryCommand;
    private final String recoveryDirectories;
    public final long recoveryPointInTime;

    public CommitLogArchiver() throws ConfigurationException
    {
        Properties commitlog_commands = new Properties();
        InputStream stream = null;
        try
        {
            stream = getClass().getClassLoader().getResourceAsStream("commitlog_archiving.properties");
            commitlog_commands.load(stream);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to load commitlog_archiving.properties", e);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }

        this.archiveCommand = commitlog_commands.getProperty("archive_command");
        this.recoveryCommand = commitlog_commands.getProperty("recovery_command");
        this.recoveryDirectories = commitlog_commands.getProperty("recovery_directories");
        String targetTime = commitlog_commands.getProperty("recovery_point_in_time");
        try
        {
            this.recoveryPointInTime = Strings.isNullOrEmpty(targetTime) ? Long.MAX_VALUE : new SimpleDateFormat("yyyy:MM:dd HH:mm:ss").parse(targetTime).getTime();
        }
        catch (ParseException e)
        {
            throw new ConfigurationException("Unable to parse recovery target time", e);
        }

        if (!Strings.isNullOrEmpty(archiveCommand) && DatabaseDescriptor.recycleCommitLog())
            throw new ConfigurationException("Commitlog recycling and archiving both enabled -- disable one or the other");
    }

    public void maybeArchive(final String path, final String name)
    {
        if (Strings.isNullOrEmpty(archiveCommand))
            return;

        archivePending.add(name);
        new Thread()
        {
            public void run()
            {
                try
                {
                    String command = archiveCommand.replace("%name", name);
                    command = command.replace("%path", path);
                    execute(command);
                }
                catch (IOException ex)
                {
                    logger.error("Unable to execute archive command", ex);
                }
                finally
                {
                    archivePending.remove(name);
                }
            }
        }.start();
    }

    public boolean maybeWaitForArchiving(String name)
    {
        while (archivePending.contains(name))
        {
            try
            {
                // sleep for 100 Millis
                Thread.sleep(100);
            }
            catch (InterruptedException ex)
            {
                // ignore.
            }
        }
        return true;
    }

    public void maybeRestoreArchive() throws IOException
    {
        if (Strings.isNullOrEmpty(recoveryDirectories))
            return;

        for (String dir : recoveryDirectories.split(","))
        {
            File[] files = new File(dir).listFiles();
            for (File fromFile : files)
            {
                File toFile = new File(DatabaseDescriptor.getCommitLogLocation(),
                                       CommitLogSegment.FILENAME_PREFIX +
                                       System.nanoTime() +
                                       CommitLogSegment.FILENAME_EXTENSION);
                String command = recoveryCommand.replace("%from", fromFile.getPath());
                command = command.replace("%to", toFile.getPath());       
                execute(command);
            }
        }
    }

    private void execute(String command) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder(command.split(" "));
        pb.redirectErrorStream(true);
        CLibrary.exec(pb);
    }
}
