/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.HashMap;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleIOManager;
import org.dejave.attica.storage.TupleComparator;

import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Sizes;

import org.dejave.attica.storage.FileUtil;


/**
 * ExternalSort: Your implementation of sorting.
 *
 * @author sviglas
 */
public class ExternalSort extends UnaryOperator {
    
    /** The storage manager for this operator. */
    private StorageManager sm;
    
    /** The name of the temporary file for the output. */
    private String outputFile;
	
    /** The manager that undertakes output relation I/O. */
    private RelationIOManager outputMan;
	
    /** The slots that act as the sort keys. */
    private int [] slots;
	
    /** Number of buffers (i.e., buffer pool pages and 
     * output files). */
    private int buffers;

    /** Iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable tuple list for returns. */
    private List<Tuple> returnList;

    private TupleComparator comparator;

    
    /**
     * Constructs a new external sort operator.
     * 
     * @param operator the input operator.
     * @param sm the storage manager.
     * @param slots the indexes of the sort keys.
     * @param buffers the number of buffers (i.e., run files) to be
     * used for the sort.
     * @throws EngineException thrown whenever the sort operator
     * cannot be properly initialized.
     */
    public ExternalSort(Operator operator, StorageManager sm,
                        int [] slots, int buffers) 
	throws EngineException {
        
        super(operator);
        this.sm = sm;
        this.slots = slots;
        comparator = new TupleComparator(slots);
        this.buffers = buffers;
        try {
            // create the temporary output files
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not instantiate external sort",
                                      sme);
        }
    } // ExternalSort()
	

    /**
     * Initialises the temporary files, according to the number
     * of buffers.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        // in the event of an error
        //
        // for the time being, the only file we
        // know of is the output file
        //
        ////////////////////////////////////////////
        outputFile = FileUtil.createTempFileName();
    } // initTempFiles()

    private ArrayList<String> generateRunsViaReplacementSelection(Relation relation) throws StorageManagerException, EngineException {
        ArrayList<String> runFiles = new ArrayList<String>();

        Tuple nextTuple = getInputOperator().getNext();

        // Find out how many Tuples we can initialize our heap with.
        int tupleSize = TupleIOManager.byteSize(relation, nextTuple);
        // Reserved: 1 Buffer for RelationalIO Input, 1 buffer for Page output
        int heapBudget = (buffers - 2) * Sizes.PAGE_SIZE;
        int initialHeapTupCount = heapBudget / tupleSize;

        // Initialize the Priority Queue(s)
        // The lack of O(n) #heapify with a custom comparator in Java is incredibly upsetting.
        PriorityQueue<Tuple> thisQ = new PriorityQueue<Tuple>(initialHeapTupCount, comparator);
        for (int added = 0; added < initialHeapTupCount; added++) {
            if (nextTuple instanceof EndOfStreamTuple) break;
            thisQ.add(nextTuple);
            nextTuple = getInputOperator().getNext();
        }

        //Empty Priority Queue to hold the tuples spilling into next Run
        PriorityQueue<Tuple> nextQ = new PriorityQueue<Tuple>(1, comparator);

        // Initialize the first Run
        String currentRunFilename = FileUtil.createTempFileName();
        sm.createFile(currentRunFilename);
        runFiles.add(currentRunFilename);
        RelationIOManager currentRunManager = new RelationIOManager(sm, relation, currentRunFilename);

        // Main Replacement-Selection loop
        while (thisQ.peek() != null) {
            // Write the lowest key from current Queue into current Run.
            Tuple lowest = thisQ.poll();
            currentRunManager.insertTuple(lowest);

            // Insert next input tuple into correct Priority Queue.
            if (!(nextTuple instanceof EndOfStreamTuple)) {
                if (comparator.compare(nextTuple, lowest) >= 0) {
                    thisQ.add(nextTuple);
                } else {
                    nextQ.add(nextTuple);
                }
                nextTuple = getInputOperator().getNext();
            }

            // If current Queue exhausted, swap Queues.
            if (thisQ.peek() == null) {
                PriorityQueue<Tuple> temp = thisQ;
                thisQ = nextQ;
                nextQ = temp;

              // Start new Run for next iteration if we have not finished.
                if (thisQ.peek() != null) {
                    currentRunFilename = FileUtil.createTempFileName();
                    sm.createFile(currentRunFilename);
                    runFiles.add(currentRunFilename);
                    currentRunManager = new RelationIOManager(sm, relation, currentRunFilename);
                }
            }
        }

        return runFiles;
    }

    public String mergeRunFiles(Relation relation, ArrayList<String> runFiles) throws java.io.IOException, StorageManagerException {
        String mergedFile;

        ArrayList<String> thisStage = runFiles;
        ArrayList<String> tempFiles = new ArrayList<String>();
        ArrayList<String> nextStage = new ArrayList<String>();

        // Repeatedly perform (B-1)-way Linear Merge in stages until a single merged file remains.
        while (!thisStage.isEmpty()) {
            // Take up to B-1 run Files for simultaneous merge
            int sliceIndex = Math.min(buffers - 2, thisStage.size());
            List<String> runSlice = thisStage.subList(0, sliceIndex);

            // Output file for merge step
            String mergeFilename = FileUtil.createTempFileName();
            sm.createFile(mergeFilename);
            tempFiles.add(mergeFilename);
            nextStage.add(mergeFilename);
            RelationIOManager mergedFileManager = new RelationIOManager(sm, relation, mergeFilename);

            // Streams and Priority Queue for efficient Merge
            HashMap<Integer, Iterator<Tuple>> streamMap = new HashMap<Integer, Iterator<Tuple>>();
            PriorityQueue<Tuple> frontier = new PriorityQueue<Tuple>(sliceIndex + 1, comparator);

            // The Frontier is initially the head of each Stream
            for (String fileName : runSlice) {
                Iterator<Tuple> stream = new RelationIOManager(sm, relation, fileName).tuples().iterator();
                tempFiles.add(fileName);

                // No need to include exhausted streams
                if (stream.hasNext()){
                    Tuple head = stream.next();
                    int tupleHash = head.hashCode();
                    streamMap.put(tupleHash, stream);
                    frontier.add(head);
                }
            }

            // Linear-merge-of-streams loop
            while (frontier.peek() != null) {
                // Remove lowest from PQ
                Tuple lowest = frontier.poll();
                // GOTCHA: Take hashCode before writing to new file since this MODIFIES TUPLE HASHCODE!
                int tupleHash = lowest.hashCode();
                mergedFileManager.insertTuple(lowest);

                Iterator<Tuple> incrementedStream = streamMap.remove(tupleHash);

                // No need to include exhausted streams
                if (incrementedStream.hasNext()) {
                    Tuple head = incrementedStream.next();
                    streamMap.put(head.hashCode(), incrementedStream);
                    frontier.add(head);
                }
            }

            // Remove the processed slice from consideration of the next iteration.
            runSlice.clear();

            // Cannot merge a single Run.
            if (thisStage.size() == 1) {
                // Overlap the remaining Run into the next stage.
                nextStage.add(thisStage.get(0));
                if (nextStage.size() > 1 ) {
                    thisStage = nextStage;
                    nextStage = new ArrayList<String>();
                    continue;

                // Exit Loop if there is no 'next'
                } else {
                    thisStage = new ArrayList<String>();
                    break;
                }
            }

            if (thisStage.isEmpty()) {
                // Another pass is needed if nextStage is not completely merged.
                if (nextStage.size() > 1) {
                    thisStage = nextStage;
                    nextStage = new ArrayList<String>();
                }
            }
        }

        mergedFile = nextStage.get(0);

        //  Clean-up all files apart from returned, merged file
        tempFiles.addAll(runFiles);
        for (String file : tempFiles) {
            if (file != mergedFile) {
                sm.deleteFile(file);
            }
        }

        return mergedFile;
    }

    /**
     * Sets up this external sort operator.
     * 
     * @throws EngineException thrown whenever there is something wrong with
     * setting this operator up
     */
    public void setup() throws EngineException {
        returnList = new ArrayList<Tuple>();
        try {
            Relation relation = getInputOperator().getOutputRelation();
            ArrayList<String> runFiles = generateRunsViaReplacementSelection(relation);
            outputFile = mergeRunFiles(relation, runFiles);

            outputMan = new RelationIOManager(sm, getOutputRelation(),
                                              outputFile);
            outputTuples = outputMan.tuples().iterator();
        }
        catch (Exception sme) {
            throw new EngineException("Could not store and sort"
                                      + "intermediate files.", sme);
        }
    } // setup()

    
    /**
     * Cleanup after the sort.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    public void cleanup () throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete the intermediate
            // files after sorting is done
            //
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // right now, only the output file is 
            // deleted
            //
            ////////////////////////////////////////////
            sm.deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not clean up final output.", sme);
        }
    } // cleanup()

    
    /**
     * The inner method to retrieve tuples.
     * 
     * @return the newly retrieved tuples.
     * @throws EngineException thrown whenever the next iteration is not 
     * possible.
     */    
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples " +
                                      "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Operator class abstract interface -- never called.
     */
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        return new ArrayList<Tuple>();
    } // innerProcessTuple()

    
    /**
     * Operator class abstract interface -- sets the ouput relation of
     * this sort operator.
     * 
     * @return this operator's output relation.
     * @throws EngineException whenever the output relation of this
     * operator cannot be set.
     */
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // setOutputRelation()

} // ExternalSort
