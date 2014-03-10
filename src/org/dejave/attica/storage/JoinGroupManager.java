package org.dejave.attica.storage;

import java.util.ArrayList;
import java.util.Iterator;

import org.dejave.attica.model.Relation;

import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Sizes;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleIOManager;

import org.dejave.attica.storage.FileUtil;


/**
 * JoinGroupManager: Abstracts the storing of a join-group's elements
 * and the recall of a join-group's tuples.
 **/
public class JoinGroupManager {

    private StorageManager sm;
    private Relation rel;
    private RelationIOManager groupMan;

    private boolean hasHitDisk;
    private int tuplesStored;
    private int tupleLimit;
    private ArrayList<Tuple> tupleBuffer;
    private String groupFilename;

    public JoinGroupManager(Relation rel, StorageManager sm, Tuple firstTup) throws StorageManagerException{
        this.sm = sm;
        this.rel = rel;
        this.hasHitDisk = false;
        this.tuplesStored = 0;
        this.tupleLimit = Sizes.PAGE_SIZE / TupleIOManager.byteSize(rel, firstTup);
        this.tupleBuffer = new ArrayList<Tuple>();
    }

    private void hitDisk() throws StorageManagerException {
        hasHitDisk = true;
        groupFilename = FileUtil.createTempFileName();
        sm.createFile(groupFilename);
        groupMan = new RelationIOManager(sm, rel, groupFilename);
        for (Tuple tup : tupleBuffer) groupMan.insertTuple(tup);
        tupleBuffer.clear();
    }

    public void insertTuple(Tuple t) throws StorageManagerException {
        if (hasHitDisk) {
            groupMan.insertTuple(t);

        } else {
            if (tuplesStored < tupleLimit) {
                tupleBuffer.add(t);
                tuplesStored++;

            } else {
                hitDisk();
                groupMan.insertTuple(t);
            }
        }
    }

    public Iterable<Tuple> tuples() throws java.io.IOException, StorageManagerException {
        if (hasHitDisk) {
            return groupMan.tuples();
        } else {
            return tupleBuffer;
        }
    }

    public void close() throws StorageManagerException {
        if (hasHitDisk) sm.deleteFile(groupFilename);
    }
}
