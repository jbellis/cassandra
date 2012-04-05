package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

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
    private final long recoveryTargetTime;

    public CommitLogArchiver()
    {
        Properties commitlog_commands = new Properties();
        FileReader commandFile = null;
        try
        {
            ClassLoader loader = CommitLogArchiver.class.getClassLoader();
            URL url = loader.getResource("commitlog_archiving.properties");
            commandFile = new FileReader(url.getFile());
            commitlog_commands.load(commandFile);
            this.archiveCommand = commitlog_commands.getProperty("archive_command");
            this.recoveryCommand = commitlog_commands.getProperty("recovery_command");
            this.recoveryDirectories = commitlog_commands.getProperty("recovery_directories");
            String targetTime = commitlog_commands.getProperty("recovery_target_time");
            this.recoveryTargetTime = Strings.isNullOrEmpty(targetTime) ? 0 : new SimpleDateFormat("MM:dd:yyyy HH:mm:ss").parse(targetTime).getTime();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
        finally
        {
            FileUtils.closeQuietly(commandFile);
        }
    }

    public void archive(final String path, final String name)
    {
        if (Strings.isNullOrEmpty(archiveCommand) || DatabaseDescriptor.recycleCommitLog())
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

    public boolean waitForArchiving(String name)
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

    public void restoreArchive() throws IOException
    {
        if (Strings.isNullOrEmpty(recoveryDirectories))
            return;

        for (String dir : recoveryDirectories.split(","))
        {
            File[] files = new File(dir).listFiles();
            for (File fromFile : files)
            {
                File toFile = new File (DatabaseDescriptor.getCommitLogLocation(), 
                                            CommitLogSegment.FILENAME_PREFIX + 
                                            System.nanoTime() + 
                                            CommitLogSegment.FILENAME_EXTENSION);
                
                String command = recoveryCommand.replace("%from", fromFile.getPath());
                command = command.replace("%to", toFile.getPath());       
                execute(command);
            }
        }
    }

    public long restoreTarget()
    {
        return recoveryTargetTime;
    }
    
    private void execute(String command) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder(command.split(" "));
        pb.redirectErrorStream(true);
        CLibrary.exec(pb);
    }
}
