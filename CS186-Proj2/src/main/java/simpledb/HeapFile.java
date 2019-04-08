package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private TupleDesc td;
    
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");

        //generate the different id for different file
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        byte[] buf = new byte[BufferPool.PAGE_SIZE];
        Page wantedPage = null;

        // some code goes here
        try{
            InputStream is = new FileInputStream(f);

            //skip to get the wanted data according to pageid            
            /*
                is.skip() method can skip the specific bytes from file head,
                so, we use skip method to make offset from head.
            */
            int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
            if(offset > 0)  is.skip(offset);

            //read data
            is.read(buf);
            wantedPage = new HeapPage((HeapPageId)pid,buf);
            is.close();

        }catch (IOException e){
            //throw new IOException("fail read page!");
            e.printStackTrace();
        }

        return wantedPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(f.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new FileIterator(tid);

    }

    public class FileIterator implements DbFileIterator {
        private ArrayList<Tuple> tuples = null;
        private int totalLen = 0;
        private int index = 0;
        private TransactionId tid = null;

        public FileIterator(TransactionId tid){
            this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException {
            this.tuples = new ArrayList<Tuple>();

            for (int i = 0; i < numPages(); i++){
                HeapPageId pid = new HeapPageId(getId(),i);
                
                //read page from BufferPool
                HeapPage tempPage = (HeapPage) Database.getBufferPool().getPage(tid,pid,null);
                
                //add iterator
                Iterator<Tuple> iter = tempPage.iterator();
                while (iter.hasNext()){
                    tuples.add(iter.next());
                }
            }  

            totalLen = tuples.size();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            if (index >= totalLen) return false;
            return true;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return tuples.get(index++);
        }

        public void rewind() throws DbException, TransactionAbortedException {
            index = 0;
        }

        public void close() {
            tuples = null;
            totalLen = 0;
            index = 0;
        }

    }

}

