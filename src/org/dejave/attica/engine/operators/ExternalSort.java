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

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleComparator;

import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Sizes;

import org.dejave.attica.storage.PageIdentifier;
import org.dejave.attica.storage.Page;

import org.dejave.attica.storage.FileUtil;

import java.lang.instrument.Instrumentation;
import java.util.PriorityQueue;

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

    private static Instrumentation globalInstrumentation;

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

    private ArrayList<String> doReplacementSelection(Relation relation) throws StorageManagerException, EngineException {
      ArrayList<String> runFiles = new ArrayList<String>();

      // FIXME: Making the assumption that at least one tuple is in stream.
      Tuple nextTuple = getInputOperator().getNext();

      // Find out how many Tuples we can initialize our heap with.
      int tupleSize = (int) globalInstrumentation.getObjectSize(nextTuple);
      Sizes sizeConstants = new Sizes();
      // Reserved: 1 Buffer for RelationalIO Input, 1 buffer for RelationalIO output
      int heapBudget = (buffers - 2) * sizeConstants.PAGE_SIZE;
      int initialHeapTupCount = heapBudget / tupleSize;

      // Initialize the Priority Queue(s)
      // The lack of O(n) #heapify with a custom comparator is incredibly upsetting.
      PriorityQueue<Tuple> thisQ = new PriorityQueue<Tuple>(initialHeapTupCount, comparator);
      for (int added = 0; added < initialHeapTupCount; added++) {
        if (nextTuple instanceof EndOfStreamTuple) break;
        thisQ.add(nextTuple);
        nextTuple = getInputOperator().getNext();
      }
      PriorityQueue<Tuple> nextQ = new PriorityQueue<Tuple>(0, comparator);

      // Initialize the first Run
      String currentRunFilename = FileUtil.createTempFileName();
      runFiles.add(currentRunFilename);
      int runPageOffset = 0;
      PageIdentifier currentRunPageID = new PageIdentifier(currentRunFilename, runPageOffset);
      Page currentRunPage = new Page(relation, currentRunPageID);
      RelationIOManager currentRunManager = new RelationIOManager(
        sm,
        relation,
        currentRunFilename
      );


      while (thisQ.peek() != null) {
        // Write the lowest key from current Queue into current Run.
        Tuple lowest = thisQ.poll();
        // If page is full, write to disk and start new page.
        if (!currentRunPage.hasRoom(lowest)) {
          sm.writePage(currentRunPage);
          runPageOffset++;
          currentRunPageID = new PageIdentifier(currentRunFilename, runPageOffset);
          currentRunPage = new Page(relation, currentRunPageID);
        }
        currentRunPage.addTuple(lowest);

        // Insert next input tuple into correct Priority Queue.
        if (! (nextTuple instanceof EndOfStreamTuple)) {
          if (comparator.compare(nextTuple, lowest) >= 0) {
            thisQ.add(nextTuple);
          } else {
            nextQ.add(nextTuple);
          }
          nextTuple = getInputOperator().getNext();
        }

        // If current Queue exhausted, swap Queues and start new Run if !finished.
        if (thisQ.peek() == null) {
          PriorityQueue<Tuple> temp = thisQ;
          thisQ = nextQ;
          nextQ = temp;

          if (thisQ.peek() != null) {
            currentRunFilename = FileUtil.createTempFileName();
            runFiles.add(currentRunFilename);
            runPageOffset = 0;
            currentRunPageID = new PageIdentifier(currentRunFilename, runPageOffset);
            currentRunPage = new Page(relation, currentRunPageID);
          }
        }
      }
      // Write final page.
      sm.writePage(currentRunPage);

      return runFiles;
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
              ArrayList<String> runFiles = doReplacementSelection(getInputOperator().getOutputRelation());


            
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////
            
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
