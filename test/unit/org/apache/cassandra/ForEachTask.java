/*
 * Copyright (c) 2001-2004 Ant-Contrib project.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra;

import java.io.File;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.TaskContainer;
import org.apache.tools.ant.taskdefs.CallTarget;
import org.apache.tools.ant.taskdefs.Property;
import org.apache.tools.ant.types.Mapper;
import org.apache.tools.ant.types.Path;
import org.apache.tools.ant.util.FileNameMapper;

/***
 * Task definition for the foreach task.  The foreach task iterates
 * over a list, a list of filesets, or both.
 *
 * <pre>
 *
 * Usage:
 *
 *   Task declaration in the project:
 *   <code>
 *     &lt;taskdef name="foreach" classname="net.sf.antcontrib.logic.ForEach" /&gt;
 *   </code>
 *
 *   Call Syntax:
 *   <code>
 *     &lt;foreach list="values" target="targ" param="name"
 *                 [parallel="true|false"]
 *                 [delimiter="delim"] /&gt;
 *   </code>
 *
 *   Attributes:
 *         list      --> The list of values to process, with the delimiter character,
 *                       indicated by the "delim" attribute, separating each value
 *         target    --> The target to call for each token, passing the token as the
 *                       parameter with the name indicated by the "param" attribute
 *         param     --> The name of the parameter to pass the tokens in as to the
 *                       target
 *         delimiter --> The delimiter string that separates the values in the "list"
 *                       parameter.  The default is ","
 *         parallel  --> Should all targets execute in parallel.  The default is false.
 *         trim      --> Should we trim the list item before calling the target?
 *
 * </pre>
 * @author <a href="mailto:mattinger@yahoo.com">Matthew Inger</a>
 */
public class ForEachTask extends Task
{
    private String list;
    private String param;
    private String delimiter;
    private String target;
    private boolean inheritAll;
    private boolean inheritRefs;
    private Path currPath;
    private boolean parallel;
    private boolean trim;
    private int maxThreads;
    private Mapper mapper;

    /***
     * Default Constructor
     */
    public ForEachTask()
    {
        super();
        this.list = null;
        this.param = null;
        this.delimiter = ",";
        this.target = null;
        this.inheritAll = false;
        this.inheritRefs = false;
        this.parallel = false;
        this.maxThreads = 5;
    }

    private void executeParallel(Vector tasks)
    {
        ExecutorService pool = Executors.newFixedThreadPool(maxThreads);
        Enumeration e = tasks.elements();
        Runnable r = null;

        // start each task in its own thread, using the
        // pool to ensure that we don't exceed the maximum
        // amount of threads
        while (e.hasMoreElements())
        {
            // Create the Runnable object
            final Task task = (Task)e.nextElement();
            r = new Runnable()
            {
                public void run()
                {
                    task.execute();
                }
            };

            // Get a thread, and start the task.
            // If there is no thread available, this will
            // block until one becomes available
            try
            {
                pool.submit(r);
            }
            catch (Exception ex)
            {
                throw new BuildException(ex);
            }

        }

        // Wait for all threads to finish before we
        // are allowed to return.
        pool.shutdown();
        try
        {
            pool.awaitTermination(1, TimeUnit.HOURS);
        }
        catch (InterruptedException ex)
        {
            throw new BuildException(ex);
        }
    }

    private void executeSequential(Vector tasks)
    {
        TaskContainer tc = (TaskContainer) getProject().createTask("sequential");
        Enumeration e = tasks.elements();
        Task t = null;
        while (e.hasMoreElements())
        {
            t = (Task)e.nextElement();
            tc.addTask(t);
        }

        ((Task)tc).execute();
    }

    public void execute()
    throws BuildException
    {
        if (list == null && currPath == null) {
            throw new BuildException("You must have a list or path to iterate through");
        }
        if (param == null)
            throw new BuildException("You must supply a property name to set on each iteration in param");
        if (target == null)
            throw new BuildException("You must supply a target to perform");

        Vector values = new Vector();

        // Take Care of the list attribute
        if (list != null)
        {
            StringTokenizer st = new StringTokenizer(list, delimiter);

            while (st.hasMoreTokens())
            {
                String tok = st.nextToken();
                if (trim) tok = tok.trim();
                values.addElement(tok);
            }
        }

        String[] pathElements = new String[0];
        if (currPath != null) {
            pathElements = currPath.list();
        }

        for (int i=0;i<pathElements.length;i++)
        {
            if (mapper != null)
            {
                FileNameMapper m = mapper.getImplementation();
                String mapped[] = m.mapFileName(pathElements[i]);
                for (int j=0;j<mapped.length;j++)
                    values.addElement(mapped[j]);
            }
            else
            {
                values.addElement(new File(pathElements[i]));
            }
        }

        Vector tasks = new Vector();

        int sz = values.size();
        CallTarget ct = null;
        Object val = null;
        Property p = null;

        for (int i = 0; i < sz; i++) {
            val = values.elementAt(i);
            ct = createCallTarget();
            p = ct.createParam();
            p.setName(param);

            if (val instanceof File)
                p.setLocation((File)val);
            else
                p.setValue((String)val);

            tasks.addElement(ct);
        }

        if (parallel && maxThreads > 1)
        {
            executeParallel(tasks);
        }
        else
        {
            executeSequential(tasks);
        }
    }

    public void setTrim(boolean trim)
    {
        this.trim = trim;
    }

    public void setList(String list)
    {
        this.list = list;
    }

    public void setDelimiter(String delimiter)
    {
        this.delimiter = delimiter;
    }

    public void setParam(String param)
    {
        this.param = param;
    }

    public void setTarget(String target)
    {
        this.target = target;
    }

    public void setParallel(boolean parallel)
    {
        this.parallel = parallel;
    }

    /**
     * Corresponds to <code>&lt;antcall&gt;</code>'s <code>inheritall</code>
     * attribute.
     */
    public void setInheritall(boolean b) {
        this.inheritAll = b;
    }

    /**
     * Corresponds to <code>&lt;antcall&gt;</code>'s <code>inheritrefs</code>
     * attribute.
     */
    public void setInheritrefs(boolean b) {
        this.inheritRefs = b;
    }


    /***
     * Set the maximum amount of threads we're going to allow
     * at once to execute
     * @param maxThreads
     */
    public void setMaxThreads(int maxThreads)
    {
        this.maxThreads = maxThreads;
    }


    public Path createPath() {
        if (currPath == null) {
            currPath = new Path(getProject());
        }
        return currPath;
    }

    public Mapper createMapper()
    {
        mapper = new Mapper(getProject());
        return mapper;
    }

    private CallTarget createCallTarget() {
        CallTarget ct = (CallTarget) getProject().createTask("antcall");
        ct.setOwningTarget(getOwningTarget());
        ct.init();
        ct.setTarget(target);
        ct.setInheritAll(inheritAll);
        ct.setInheritRefs(inheritRefs);
        return ct;
    }

    protected void handleOutput(String line)
    {
        try {
            super.handleOutput(line);
        }
        // This is needed so we can run with 1.5 and 1.5.1
        catch (IllegalAccessError e) {
            super.handleOutput(line);
        }
    }

    protected void handleErrorOutput(String line)
    {
        try {
            super.handleErrorOutput(line);
        }
        // This is needed so we can run with 1.5 and 1.5.1
        catch (IllegalAccessError e) {
            super.handleErrorOutput(line);
        }
    }

}


