/*
 * Created on Feb 11, 2004 by sviglas
 *
 * Modified on Feb 17, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.io.IOException;

import org.dejave.attica.model.Relation;

import org.dejave.attica.engine.predicates.Predicate;
import org.dejave.attica.engine.predicates.PredicateEvaluator;
import org.dejave.attica.engine.predicates.PredicateTupleInserter;

import org.dejave.attica.storage.IntermediateTupleIdentifier;
import org.dejave.attica.storage.JoinGroupManager;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;

import org.dejave.attica.storage.FileUtil;

/**
 * MergeJoin: Implements a merge join. The assumptions are that the
 * input is already sorted on the join attributes and the join being
 * evaluated is an equi-join.
 *
 * @author sviglas
 * 
 */
public class MergeJoin extends NestedLoopsJoin {
	
    /** The name of the temporary file for the output. */
    private String outputFile;
    
    /** The relation manager used for I/O. */
    private RelationIOManager outputMan;
    
    /** The pointer to the left sort attribute. */
    private int leftSlot;
	
    /** The pointer to the right sort attribute. */
    private int rightSlot;

    /** The iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable output list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new mergejoin operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @param leftSlot pointer to the left sort attribute.
     * @param rightSlot pointer to the right sort attribute.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
    public MergeJoin(Operator left, 
                     Operator right,
                     StorageManager sm,
                     int leftSlot,
                     int rightSlot,
                     Predicate predicate) 
	throws EngineException {
        
        super(left, right, sm, predicate);
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        returnList = new ArrayList<Tuple>(); 
        try {
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not instantiate " +
                                                     "merge join");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // MergeJoin()


    /**
     * Initialise the temporary files -- if necessary.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        //
        ////////////////////////////////////////////
        outputFile = FileUtil.createTempFileName();
        getStorageManager().createFile(outputFile);
    } // initTempFiles()

    void doMergeJoin() throws IOException, StorageManagerException, EngineException {
        // Initialize both 'pointers' as the beginning of streams
        Tuple lTuple = getInputOperator(LEFT ).getNextNonNull();
        Tuple rTuple = getInputOperator(RIGHT).getNextNonNull();
        try {
            while (true) {
                // If either Operator is empty, the join cannot continue...
                if ((lTuple instanceof EndOfStreamTuple) || (rTuple instanceof EndOfStreamTuple)) {
                    return;
                }
                // ...Otherwise, the join algorithm proceeds.

                // Advance LEFT relation whilst it trails RIGHT
                while (lTuple.getValue(leftSlot).compareTo(rTuple.getValue(rightSlot)) < 0) {
                    lTuple = getInputOperator(LEFT).getNextNonNull();
                    if (lTuple instanceof EndOfStreamTuple) return;
                }

                // Advance Right relation whilst it trails LEFT
                while (rTuple.getValue(rightSlot).compareTo(lTuple.getValue(leftSlot)) < 0) {
                    rTuple = getInputOperator(RIGHT).getNextNonNull();
                    if (rTuple instanceof EndOfStreamTuple) return;
                }

                // While slots are equal:
                //      Increment RIGHT, emitting new tuples as join
                //      but also storing the current 'group'
                Tuple groupVal = lTuple;
                JoinGroupManager groupManager = new JoinGroupManager(getInputOperator(RIGHT).getOutputRelation(),
                        getStorageManager(), rTuple);
                try {
                    while (lTuple.getValue(leftSlot).compareTo(rTuple.getValue(rightSlot)) == 0) {
                        groupManager.insertTuple(rTuple);
                        outputMan.insertTuple(combineTuples(lTuple, rTuple));
                        rTuple = getInputOperator(RIGHT).getNextNonNull();
                        if (rTuple instanceof EndOfStreamTuple) break;
                    }

                    // Advance LEFT now that the group has been exhausted
                    lTuple = getInputOperator(LEFT).getNextNonNull();
                    if (lTuple instanceof EndOfStreamTuple) return;

                    // If LEFT has not changed despite the increment:
                    //      Produce another set of join tuples
                    //      for the previous group.
                    while (lTuple.getValue(leftSlot).compareTo(groupVal.getValue(leftSlot)) == 0) {
                        for (Tuple groupTuple : groupManager.tuples()) {
                            outputMan.insertTuple(combineTuples(lTuple, groupTuple));
                        }

                        // Increment LEFT before checking again
                        lTuple = getInputOperator(LEFT).getNextNonNull();
                        if (lTuple instanceof EndOfStreamTuple) return;
                    }
                } finally {
                    groupManager.close();
                }
            }
        } finally {
            // Any open streams are closed.
            getInputOperator(LEFT).close();
            getInputOperator(RIGHT).close();
        }
    }

    /**
     * Sets up this merge join operator.
     * 
     * @throws EngineException thrown whenever there is something
     * wrong with setting this operator up.
     */
    
    @Override
    protected void setup() throws EngineException {
        try {
            outputMan = new RelationIOManager(getStorageManager(),
                                              getOutputRelation(),
                                              outputFile);

            doMergeJoin();

            // open the iterator over the output
            outputTuples = outputMan.tuples().iterator();
        }
        catch (IOException ioe) {
            throw new EngineException("Could not create page/tuple iterators.",
                                      ioe);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not store " + 
                                                     "intermediate relations " +
                                                     "to files.");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // setup()
    
    
    /**
     * Cleans up after the join.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    @Override
    protected void cleanup() throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete any temporary files
            //
            ////////////////////////////////////////////
            
            getStorageManager().deleteFile(super.leftFile);
            getStorageManager().deleteFile(super.rightFile);
            getStorageManager().deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not clean up " +
                                                     "final output");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // cleanup()

    /**
     * Inner method to propagate a tuple.
     * 
     * @return an array of resulting tuples.
     * @throws EngineException thrown whenever there is an error in
     * execution.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Inner tuple processing.  Returns an empty list but if all goes
     * well it should never be called.  It's only there for safety in
     * case things really go badly wrong and I've messed things up in
     * the rewrite.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        return new ArrayList<Tuple>();
    }  // innerProcessTuple()

    
    /**
     * Textual representation
     */
    protected String toStringSingle () {
        return "mj <" + getPredicate() + ">";
    } // toStringSingle()

} // MergeJoin
