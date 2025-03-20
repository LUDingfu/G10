#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    int cycles = 0;
    // Allow two full cycles: 
    // in the first, clear refbits,
    // in the second, candidates are eligible if not pinned.
    while (cycles < 2) {
        // Scan all buffer frames
        for (int i = 0; i < numBufs; i++) {
            // advance clock hand
            clockHand = (clockHand + 1) % numBufs;
            BufDesc & candidate = bufTable[clockHand];

            // If not valid, it's free -- use it immediately.
            if (!candidate.valid) {
                
                frame = clockHand;
                return OK;
            }

            // If the refbit is set during the first cycle, clear it.
            if (candidate.refbit) {
                candidate.refbit = false;
                continue;
            }
            
            // If frame is pinned, skip to next.
            if (candidate.pinCnt > 0)
                continue;
            
            // At this point, candidate is not pinned and its refbit is false.
            // If dirty, write it to disk.
            if (candidate.dirty) {
                Status status = candidate.file->writePage(candidate.pageNo, &bufPool[clockHand]);
                if (status != OK)
                    return UNIXERR;
                candidate.dirty = false;
            }
            
            // Remove mapping if the candidate was valid.
            if (candidate.valid)
                hashTable->remove(candidate.file, candidate.pageNo);
            
            // Reinitialize the descriptor and return the frame.
            frame = clockHand;
            return OK;
        }
        cycles++;
    }
    printSelf();
    // If no candidate found even after two cycles then all buffers are pinned.
    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    // check whether it is already in the buffer pool and get the frame number
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status == OK) { // Case: found in the buffer pool
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++; 
        page = &bufPool[frameNo]; 
        return OK;
    }
    // If the lookup failed because of a hash table error, propagate HASHTBLERROR
    if (status == HASHTBLERROR)
        return HASHTBLERROR;

    // Case: not in the buffer pool, try to allocate a new buffer frame.
    status = allocBuf(frameNo);
    if (status != OK)
        return status; // returns BUFFEREXCEEDED if all buffers are pinned

    // Read the page from disk.
    status = file->readPage(PageNo, &bufPool[frameNo]);
    if (status != OK)
        return UNIXERR; // Propagate the Unix I/O error as UNIXERR

    // Insert the page mapping into the hash table.
    status = hashTable->insert(file, PageNo, frameNo);
    if (status != OK)
        return HASHTBLERROR;

    // Set up the buffer frame.
    bufTable[frameNo].Set(file, PageNo);
    page = &bufPool[frameNo];
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    
   
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    // return HASHNOTFOUND if the page is not in the buffer pool hash table
    if (status != OK) {
        return HASHNOTFOUND;
    }

    // if dirty is true, sets the dirty bit of the frame
    // returns PAGENOTPINNED if the pin count is already 0
    BufDesc& bufDesc = bufTable[frameNo];
    if (bufDesc.pinCnt <= 0)
        return PAGENOTPINNED;
    // decrements the pinCnt of the frame
    bufDesc.pinCnt--;
    if (dirty)
        bufDesc.dirty = true;
    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status = file->allocatePage(pageNo);
    if (status != OK)
        return status;
    
    int frameNo;
    status = allocBuf(frameNo);
    if (status != OK)
        return status;
    
    // Set up the buffer frame.
    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];

    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK)
        return HASHTBLERROR;
    
    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


