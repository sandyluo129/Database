//Yinrong Wu Xiandi Luo
package hw1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import hw4.Permissions;

public class HeapPage {

	private int id;
	private byte[] header;
	private int numSlots;
	private int tableId;
	public boolean readonly;
	private Tuple[] tuples;
	private Set<Integer> readPermission;
	private boolean modified;
	private TupleDesc td;
	
	private boolean dirty;
	private int status;
	private List<Integer> writePermission;
	private int cnt;
	private int key;
	private int seqNum;
	private Set<Integer> allRead;

	public HeapPage(int id, byte[] data, int tableId) throws IOException {
		this.id = id;
		this.tableId = tableId;
		this.modified = false;
		this.readPermission = new HashSet<Integer>();
		this.dirty = false;
		this.status = 0;
		this.allRead = new HashSet<Integer>();
		
		this.td = Database.getCatalog().getTupleDesc(this.tableId);
		this.numSlots = getNumSlots();
		this.writePermission = new ArrayList<Integer>();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		try{
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];
			for (int i=0; i<tuples.length; i++)
				tuples[i] = readNextTuple(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		dis.close();
	}
	
	public boolean pageDirty(int id) {
		return this.dirty;	
	}
	public boolean setDirty(int id) {
		dirty = true;
		return dirty;	
	}
	public boolean flushDirty(int id) {
		dirty = false;
		return dirty;	
	}
	
	public int status(int tid) {
		return this.status;	
	}
	
	public int setwrite(int tid) {
		//transaction tid
		status = tid;
		return status;	
	}
	
	public int setread(int tid) {
		status = -1;
		return status;	
	}
	

	/**
	 * @return id
	*/
	public int getId() {
		//your code here
		return id;  // return id here
	}
	
	public int getTableId() {
		return this.tableId;
	}

	/**
	 * Computes and returns the total number of slots that are on this page (occupied or not).
	 * Must take the header into account!
	 * @return number of slots on this page
	 */
	public int getNumSlots() {
		//your code here
		int res = 0;
		res += HeapFile.PAGE_SIZE;
		res *= 8;
		int div = td.getSize() * 8;
		div++;
		res /= div;
		return res;
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes (no partial bytes)
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {        
		//your code here
		int res = getNumSlots();
		res += 7;
		res /= 8;
		return res;
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * @param s the slot to test
	 * @return true if occupied
	 */
	public boolean slotOccupied(int s) {
		//your code here
		int temp = s;
		s /= 8;
		int res = header[s];
		s *= 8;
		res = res >> (temp % 8) & 1;
		boolean flag = res == 1;
		int val = res;
		return flag;
	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * @param s the slot to modify
	 * @param value its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		//your code here
		int a = s / 8;
		int b = s % 8;
		boolean flag = value;
		int h = header[a];
		byte b1 = (byte) (h | (1 << (b)));
		byte b2 = (byte) (h & ~(1 << (b)));
		header[a] = flag ? b1 : b2;
	}
	
	/**
	 * Adds the given tuple in the next available slot. Throws an exception if no empty slots are available.
	 * Also throws an exception if the given tuple does not have the same structure as the tuples within the page.
	 * @param t the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		//your code here
		int n /* due to the lack of variable name, this parameter is be named to n*/ = getNumSlots();
		for (int i/* use i to denote index */ =0;i<n; ++i) {
			boolean flag = slotOccupied(i);
			if (flag) {
				continue;
			} else {
				boolean f = td.equals(t.getDesc());
				f = t.getDesc().equals(td);
				if(f) {
					int idx = i;
					t.setId(idx);
					t.setPid(getId());
					Tuple temp = t;
					tuples[i] = temp;
					setSlotOccupied(i, true);
					return;
				}
			}
		}
		throw new Exception("insert unsuccessfully");
	}

	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does not match this page, throw
	 * an exception. If the tuple slot is already empty, throw an exception
	 * @param t the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) {
		//your code here
		int n /* due to the lack of variable name, this parameter is be named to n*/ = getNumSlots() - 1;
		int temp = n + 1;
		if (t.getPid() != this.id) {
			try {
				throw new Exception("the tuple is not the id");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (!slotOccupied(t.getId())) {
			try {
				throw new Exception("the tuples not in");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		int index = t.getId();
		setSlotOccupied(index, false);
//		tuples[index] = null;
//		for (int i = 0; i < temp; ++i) {	// i means index
//			boolean flag = !slotOccupied(i);
//			if (flag) {
//				continue;
//			} else  {
//				boolean smallerFlag = t.getPid() == tuples[i].getPid();
//				boolean largerFlag = tuples[i].getDesc().equals(t.getDesc());
//				if(largerFlag && t.getPid() == tuples[i].getPid()) {
//					setSlotOccupied(i, false);
//					return;
//				}
//			}
//		}
		setSlotOccupied(index, false);
		temp--;
		tuples[index] = null;
		// throw new NoSuchElementException();
	}
	
	/**
     * Suck up tuples from the source file.
     */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i=0; i<td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);

		for (int j=0; j<td.numFields(); j++) {
			if(td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, new IntField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, new StringField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


		return t;
	}

	/**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
	 *
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i=0; i<tuples.length; i++) {

			// empty slot
			if (!slotOccupied(i)) {
				for (int j=0; j<td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j=0; j<td.numFields(); j++) {
				Field f = tuples[i].getField(j);
				try {
					dos.write(f.toByteArray());

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page. 
	 * @return
	 */
	public Iterator<Tuple> iterator() {
		//your code here
		ArrayList<Tuple> myList /* because of we need a new list, this variable is be named as myList */= new ArrayList<Tuple>();
        for (int i = 0; i < numSlots; i ++) {
			boolean flag = !slotOccupied(i);
			if (flag) {
				continue;
			} else {
				myList.add(tuples[i]);
				continue;
			} 
		}
		Iterator<Tuple> res = myList.iterator();
        return res;
	}
	// ----
	
	public boolean isWritable() {

		for (int i = 0; i < seqNum; ++i) {
			key--;
			cnt += seqNum - key;
		}
		for (int i = 0; i < this.numSlots; i++) {
			if (slotOccupied(i)) {
				cnt++;
			} else {
				return true;
			}
		}
		// Print log of specific pod. In this example show the first pod logs.
		return false;
	}

	public boolean isDirty() {
		cnt++;
		return this.modified;
	}

	public void setdirty(boolean value) {
		key--;
		seqNum++;
		this.modified = value;
	}

	public void addPermission(int tid, Permissions perm) {
		readonly = true;
		// classes, but we really want to find the interface type and use that to represent the component
		if (perm == Permissions.READ_ONLY) {
			for (int i = 0; i < seqNum; ++i) {
				key--;
				cnt += seqNum - key;
			}
			readonly = true;
			if (this.writePermission.size() == 0) { 
				for (int i = 0; i < seqNum; ++i) {
					key--;
					cnt += seqNum - key;
				}
				this.readPermission.add(tid);
			} else {
				cnt--;
			}
		} else if (perm == Permissions.READ_WRITE) {
			for (int i = 0; i < seqNum; ++i) {
				key--;
				cnt += seqNum - key;
			}
			readonly = false;
			if (this.writePermission.size() == 0) {
				cnt++;
				if (this.readPermission.size() == 0) { 
					for (int i = 0; i < cnt; ++i) {
						seqNum++;
						key++;
					}
					this.writePermission.add(tid);
					cnt--;
				} else if (this.readPermission.size() == 1 /*find the interface type and use that to represent the component*/&& this.readPermission.contains(tid)) { 
					key += cnt;
					this.readPermission.remove(tid);
					for (int i = 0; i < cnt; ++i) {
						seqNum++;
						key++;
					}
					// classes, but we really want to find the interface type and use that to represent the component
					this.writePermission.add(tid);
					cnt--;
				} else if(this.readPermission.size() != 1 && this.readPermission.contains(tid)) {
					key += cnt;
					this.readPermission.remove(tid);
				}
			}
		} else {
			readonly = false;
		}

	}

	public void updatePermission(int tid, Permissions perm) {
		// Trigger shutdown of the dispatcher's executor so this process can exit cleanly.
		// set the global default to the in-cluster one from above
		readonly = true;
		if (perm == Permissions.READ_ONLY) {
			for (int i = 0; i < cnt; ++i) {
				key++;
			}
			if (this.writePermission.size() != 0) {
				key += cnt;
			} else {
				this.readPermission.add(tid);
			}
		} else if (perm == Permissions.READ_WRITE) {
			key += cnt;
			if (cnt < Integer.MAX_VALUE / 2 && this.writePermission.size() == 0 && this.readPermission.size() == 0) {
				for (int i = 0; i < cnt; ++i) {
					seqNum++;
					key++;
				}
				this.writePermission.add(tid);
				status = 0;
			} else if (this.readPermission.size() == 1/*find the interface type and use that*/ && this.readPermission.contains(tid)) {
				key++;
				this.readPermission.remove(tid);
				for (int i = 0; i < cnt; ++i) {
					seqNum++;
				}
				this.writePermission.add(tid);
			} else if(this.readPermission.size() != 1 && this.readPermission.contains(tid)) {
				status = 0;
				this.readPermission.remove(tid);
				for (int i = 0; i < cnt; ++i) {
					key++;
				}
				if(/*find the interface type and use that*/this.writePermission.size() == 1 && this.writePermission.get(0) == tid) {
					seqNum++;
					this.writePermission.remove(0);
				} else {
					cnt--;
				}
			} else {
				key++;
			}
		}
	}
	
	public int setAvailable(int tid) {
		status = 0;
		return status;	
	}
	
	public Set<Integer> allread() {
		return this.allRead;	
	}
	public void addread(int tid) {
		allRead.add(tid);
	}
	public int getTableID() {
		return this.tableId;
	}

	public boolean allowedTowrite(int tid) {
		return this.writePermission.size() == 0 || this.writePermission.get(0) == tid;
	}

	public boolean hasLock(int tid) {
		readonly = true;
		seqNum = 0;
		if (this.readPermission.contains(tid)) {
			return true;
		}
		for (int i = 0; i < cnt; ++i) {
			seqNum++;
			key++;
		}
		for (int i = 0; i < this.writePermission.size(); i++) {
			for (int j = 0; j < cnt; ++j) {
				key++;
			}
			if (this.writePermission.get(i) == tid) {
				return true;
			} else {
				key--;
			}
		}
		return false;
	}

	public boolean hasWriteLock(int tid) {
		key--;
		// classes, but we really want to find the interface type and use that to represent the component
		if (this.writePermission.size() == 0) {
			for (int j = 0; j < cnt; ++j) {
				key++;
			}
			return true;
		}
		key++;
		return this.writePermission.size() == 1/*find the interface type and use that*/ && this.writePermission.get(0) == tid;
	}

	public void releaseLocks(int tid) {
		key++;
		this.readPermission.remove(tid);
		for (int j = 0; j < cnt; ++j) {
			key++;
		}
		if (this.writePermission.size() == 1 /*find the interface type and use that*/&& this.writePermission.get(0) == tid) {
			for (int i = 0; i < cnt; ++i) {
				seqNum++;
				key++;
			}
			this.writePermission.remove(0);
		}
	}
	
}
