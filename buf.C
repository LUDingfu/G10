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
    Status stat;
    bool found = false;

    while (!found) {
        clockHand = (clockHand + 1) % numBufs;
        
        if (bufTable[clockHand].valid == false) {
            found = true;
            frame = clockHand;
            continue;;
        }

        if (bufTable[clockHand].refbit == true) {
            bufTable[clockHand].refbit = false;
            continue;
        }

        if (bufTable[clockHand].pinCnt > 0) {
            continue;
        }

        if (bufTable[clockHand].dirty == true) {
            if ((stat = bufTable[clockHand].file->writePage(
                    bufTable[clockHand].pageNo,&(bufPool[clockHand])))
                != OK) {
                stat = UNIXERR;
                return stat;
            }
            bufTable[clockHand].dirty = false;
        }
        
        if (bufTable[clockHand].valid) {
            hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
        }

        found = true;
        frame = clockHand;
    }

    if (!found) {
        return BUFFEREXCEEDED;
    }

    return OK;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    Status stat;
    int frameNo = 0;
    if (hashTable->lookup(file, PageNo, frameNo) != OK) {
        return UNIXERR;
    }

    if (stat == OK) {
        page = &bufPool[frameNo];
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
    } else {
        stat = allocBuf(frameNo);
        if (stat != OK) {
            return stat;
        }
        bufTable[frameNo].Set(file, PageNo);
        page = &bufPool[frameNo];
        hashTable->insert(file, PageNo, frameNo);
    }
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    Status stat;
    int frameNo = 0;
    stat = hashTable->lookup(file, PageNo, frameNo);

    if (stat == OK) {
        bufTable[frameNo].refbit = true;
        if (dirty) {
            bufTable[frameNo].dirty = true;
        }
        if (bufTable[frameNo].pinCnt <= 0) {
            return PAGENOTPINNED;
        }
        bufTable[frameNo].pinCnt--;
    } else {
        return stat;
    }
    return stat;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status stat;
    Page newPage;
    int frameNo = 0;
    if ((stat = file->allocatePage(pageNo)) != OK) {
        return stat;
    }
    if ((stat = file->readPage(pageNo,&newPage)) != OK) {
        return stat;
    }
    if ((stat = allocBuf(frameNo)) != OK) {
        return stat;
    }
    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];
    memcpy((char *)page,(char *)&newPage,PAGESIZE);
    hashTable->insert(file, pageNo, frameNo);
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


