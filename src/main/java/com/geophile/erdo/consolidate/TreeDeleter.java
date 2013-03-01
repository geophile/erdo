package com.geophile.erdo.consolidate;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.geophile.erdo.consolidate.Consolidation.Element;

public class TreeDeleter extends Thread
{
    @Override
    public void run()
    {
        List<Element> queueCopy = new ArrayList<>();
        try {
            while (!stopped) {
                synchronized (this) {
                    while (!stopped && queue.isEmpty()) {
                        wait();
                    }
                    queueCopy.addAll(queue);
                    queue.clear();    
                }
                if (!stopped) {
                    for (Element element : queueCopy) {
                        LOG.log(Level.FINE, "Destroy {0} now", element);
                        element.destroyPersistentState();
                    }
                    queueCopy.clear();
                }
            }
        } catch (InterruptedException e) {
            // Exiting
        }
    }

    public synchronized void delete(List<Element> obsolete)
    {
        if (!obsolete.isEmpty()) {
            if (LOG.isLoggable(Level.INFO)) {
                LOG.log(Level.INFO, "Deleting {0}", obsolete);
            }
            this.queue.addAll(obsolete);
            notify();
        }
    }

    public synchronized void shutdown()
    {
        stopped = true;
        notify();
    }
    
    public static TreeDeleter create()
    {
        TreeDeleter treeDeleter = new TreeDeleter();
        treeDeleter.setName("TREE_DELETER");
        treeDeleter.setDaemon(true);
        treeDeleter.start();
        return treeDeleter;
    }

    private TreeDeleter()
    {}

    // Class state

    private static final Logger LOG = Logger.getLogger(TreeDeleter.class.getName());

    // Object state

    private final Deque<Element> queue = new ArrayDeque<>();
    private boolean stopped = false;
}
