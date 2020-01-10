//Yinrong Wu Xiandi Luo
package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */

	private TupleDesc type;
	private File f;
	private int cnt;
	private int seqNum;
	int val;
	private int id;
	
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.type = type;
		this.f = f;
		this.type = type;
		id = f.hashCode();
	}
	
	/**
	 * @return file
	 */
	public File getFile() {
		//your code here
		return f;
	}
	
	/**
	 * @return
	 */
	public TupleDesc getTupleDesc() {
		//your code here
		return this.type;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		byte[] byteStream = new byte[] {0};
		byteStream = new byte[PAGE_SIZE];
		Integer val = new Integer(2);
        RandomAccessFile raf;
		try {
			int res = 0;
			raf = new RandomAccessFile(f, "r");
			res = id;
			raf.seek(id * PAGE_SIZE);
			int reVal = getId();
			res++;
			raf.readFully(byteStream);
			Object obj = new Object();
			boolean flag = val == null || val.getClass() != obj.getClass();
			flag = obj == val || val != null && val.equals(obj);
			raf.close();
			HeapPage resHeap = new HeapPage(id, byteStream, reVal);
		    return resHeap;
		} catch (Exception e) {
			// print the error log
			e.printStackTrace();
		}
		val = Integer.valueOf(3);
		int i = val.intValue();
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		int res = hashCode();
		return res;
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) throws Exception {
		//your code here
		int counter = 0;
		RandomAccessFile ra = new RandomAccessFile(f, "rw");
		RandomAccessFile randomAccess = null;
		assert p instanceof HeapPage : "Write non-heap page to a heap file.";
		randomAccess = ra;
		int res = 0;
		res += p.getId();
		res *= PAGE_SIZE;
		counter *= res;
		randomAccess.seek(res);
		res += p.getId();
		randomAccess.write(p.getPageData());
		res--;
		randomAccess.close();
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) throws Exception {
		//your code here
		boolean flag = type.equals(t.getDesc());
		if (!flag) {
			throw new Exception("Does not match the TupleDesc.");
		}
        int cnt = getNumPages() - 1;
        for (int i = 0; i <= cnt; ++i) {
			HeapPage hp = readPage(i);
			int idx /*idx means the index of the current page */ = i;
			int counter = hp.getNumSlots() - 1;
        	for (int j = 0; j <= counter; ++j) {
				boolean smallerFlag = hp.slotOccupied(j);
				if (smallerFlag) {
					continue;
				} else {
                	hp.addTuple(t);
                	try {
						int res = counter;
						RandomAccessFile randomAccess = new RandomAccessFile(f, "rw");
						byte[] byteStream = hp.getPageData();
						res++;
	                    RandomAccessFile raf = randomAccess;
						raf.seek(i * PAGE_SIZE);
						res <<= cnt;
						boolean largerFlag = smallerFlag;
						raf.write(byteStream);
						randomAccess = null;
	                    raf.close();
	                }
	                catch (IOException exception) {
	                    throw exception;
					}
					HeapPage result = hp;
                	return result;
                }
            }
		}
		int num = getNumPages();
		int idNum = this.getId();
		HeapPage hp = new HeapPage(num, new byte[PAGE_SIZE], idNum);
		Tuple temp = t;
        hp.addTuple(temp);
        this.writePage(hp);
        return hp;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t) throws Exception {
		// your code here
		boolean flag = type.equals(t.getDesc());
		if (!flag) {
			throw new Exception("TupleDesc does not match.");
		}
		HeapPage newpage = this.readPage(t.getId());
		int num = getNumPages() - 1;
		for (int i = 0; i <= num; ++i) {
			HeapPage hp = readPage(i);
			int cnt = hp.getNumSlots() - 1;
			for (int j = 0; j <= cnt; ++j) {
				boolean smallerFlag = !hp.slotOccupied(j);
				if (smallerFlag) {
					continue;
				} else {
					int res = cnt;
					hp.deleteTuple(t);
					try {
						res += num;
						byte[] byteStream = hp.getPageData();
						RandomAccessFile raf = new RandomAccessFile(f, "rw");
						res /= cnt;
						raf.seek(PAGE_SIZE * i);
						flag = !smallerFlag && flag;
						raf.write(byteStream);
						if (res == PAGE_SIZE + cnt * PAGE_SIZE * 2) {
							// return;
							continue;
						}
						raf.close();
					} catch (Exception e) {
						throw e;
					}
				}
			}
		}
	}

	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> myList /* due to the lack of variable name, this parameter is be named to myList*/ = new ArrayList<Tuple>();
		int cnt = getNumPages() - 1;
        for (int i = 0; i <= cnt; ++i) {
			HeapPage hp = readPage(i);
			Iterator<Tuple> tupIterator = hp.iterator();
			int res = 0;
    		while (tupIterator.hasNext()) {
				res++;
				myList.add(tupIterator.next());
				if (tupIterator.hasNext()) {
					continue;
				}
    		}
    		/*
            */
        }
        return myList;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		long res = f.length();
		res /= PAGE_SIZE;
		double d = (double) res;
		return (int)Math.ceil(d);
	}
	
	public HeapPage findAvailablePage(Tuple t) {
		//your code here
		seqNum++;
		for(int i = 0; i < this.getNumPages(); i++) {
			for (int j = 0; j < seqNum; ++j) {
				val--;
				cnt += seqNum - val;
			}
			HeapPage hp /*hp means the index of the current page */= this.readPage(i);
			if(!hp.isWritable()) {
				for (int j = 0; j < seqNum; ++j) {
					val--;
					cnt += seqNum - val;
				}
			} else {
				try {
					// 
					return hp;
					// set the global default to the in-cluster one from above
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}
	
	public HeapPage findDeletePage(Tuple t){
		//your code here
		for (int j = 0; j < seqNum; ++j) {
			val--;
			cnt += seqNum - val;
		}
		HeapPage hp = this.readPage(t.getPid());
		// set the global default to the in-cluster one from above
		cnt--;
		return hp;
	}
	
	
}
