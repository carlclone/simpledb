package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        int tableId = pid.getTableId();
        final int pgSize = Database.getBufferPool().getPageSize();
        byte[] rawPageData = HeapPage.createEmptyPageData();
        //open file
        try {
            FileInputStream in = new FileInputStream(file);
            //find page pos
            in.skip(pgNo * pgSize);
            //read page
            in.read(rawPageData);
            return new HeapPage(new HeapPageId(tableId, pgNo), rawPageData);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("file not found");
        } catch (IOException e) {
            throw new IllegalArgumentException("page not found");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int pgNo = pid.getPageNumber();

        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] pgData = page.getPageData();
        RandomAccessFile dbfile = new RandomAccessFile(file, "rws");
        dbfile.skipBytes(pgNo * pageSize);
        dbfile.write(pgData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // fileSize / pgSize
        int fileByteSize = (int) file.length();
        return fileByteSize / Database.getBufferPool().getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> affectedPage = new ArrayList<>(1);
        int numPages = numPages();

        for (int pgNo = 0; pgNo < numPages + 1; pgNo++) {
            HeapPageId pid = new HeapPageId(getId(), pgNo);
            HeapPage pg;
            if (pgNo < numPages) {
                pg = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
             } else {
                pg = new HeapPage(pid, HeapPage.createEmptyPageData());
            }

            if (pg.getNumEmptySlots() > 0) {
                pg.insertTuple(t);

                if (pgNo < numPages) {
                    affectedPage.add(pg);
                } else {
                    writePage(pg);
                }
                return affectedPage;
            }
        }
        throw new DbException("HeapFile: InsertTuple: Tuple can not be added");
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        HeapPageId pid = (HeapPageId) rid.getPageId();
        ArrayList<Page> affectedPage = new ArrayList<>(1);
        if (pid.getTableId() == getId()) {
            int pgNo = pid.getPageNumber();
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            pg.deleteTuple(t);
            affectedPage.add(pg);
            return affectedPage;
        }
        throw new DbException("HeapFile: deleteTuple: tuple.tableid != getId");
    }

    private class HeapFileIterator implements DbFileIterator {
        private Integer pgCursor;
        private Iterator<Tuple> tupleIterator;
        private final TransactionId tid;
        private final int tableid;
        private final int numPages;

        public  HeapFileIterator(TransactionId tid) {
            this.pgCursor = null;
            this.tupleIterator = null;
            this.tid = tid;
            this.tableid = getId();
            this.numPages = numPages();
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgCursor=0;
            tupleIterator = getTupleIterator(pgCursor);
        }

        private Iterator<Tuple> getTupleIterator(Integer pgCursor) throws TransactionAbortedException, DbException {
            //get page from Bufferpool
            HeapPageId heapPageId = new HeapPageId(tableid, pgCursor);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, heapPageId, Permissions.READ_ONLY);
                //get iterator from page
                return page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (pgCursor==null) {
                return false;
            }
            if (tupleIterator.hasNext()) {
                return true;
            }

            while (pgCursor+1 < numPages) {
                pgCursor++;
                tupleIterator = getTupleIterator(pgCursor);
                if( tupleIterator.hasNext()) {
                    return true;
                }
            }

            return false;

        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator==null) {
                throw new NoSuchElementException("iterator not open yet");
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pgCursor=0;
            tupleIterator = getTupleIterator(pgCursor);
        }

        @Override
        public void close() {
            pgCursor=null;
            tupleIterator = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

