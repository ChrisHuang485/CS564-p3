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
    Status status = OK;
    int c = 0;
    int flag = -1;


    while(c < numBufs)
    {
        advanceClock();

        if(bufTable[clockHand].valid && bufTable[clockHand].pinCnt > 0)
            c++;
        
        if(c == (int)numBufs)
            return BUFFEREXCEEDED;

        if(bufTable[clockHand].valid != true)
            break;

        if(bufTable[clockHand].refbit)
        {
            
            bufTable[clockHand].refbit = false;
            continue;
        }
        else if(bufTable[clockHand].refbit == false && bufTable[clockHand].pinCnt != 0)
        {
            continue;
        }
        else
        {
            hashTable->remove(bufTable[clockHand].file,bufTable[clockHand].pageNo); 
            flag = 1;
            break;
        }
    } 

    if(c >= numBufs && flag == -1)
        return BUFFEREXCEEDED;


    if(bufTable[clockHand].dirty == true)
    {
        
        status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);

        if(status != OK)
            return status;
        
    }

    bufTable[clockHand].Clear();
    frame = clockHand;
    return OK;
}
	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
        //Qiaoyu
    Status status;
    int frameNo = 0;
    status = hashTable->lookup(file, PageNo, frameNo);
    if (status == OK) {
            //Page is in the buffer pool
            //set the  refbit to true
        bufTable[frameNo].refbit = true;
            //increase the  pinCnt for the page
        bufTable[frameNo].pinCnt += 1;
            //return a pointer to the frame containing the page
        page = &bufPool[frameNo];

    } else if (status == HASHNOTFOUND) {
            //Page is not in the buffer pool
            //Call allocBuf() to allocate a buffer frame
        status = allocBuf(frameNo);
        if (status != OK)
            return status;

            //read the page from disk into the buffer pool frame
        status = file->readPage(PageNo, &bufPool[frameNo]);
        if (status != OK)
            return status;

            //insert the page into the hashtable
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK)
            return status;

            //invoke Set() on the frame to
        bufTable[frameNo].Set(file, PageNo);
            //return a pointer to the frame containing the page
        page = &bufPool[frameNo];

    }
    return status;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{

    int frameNo = 0;
    Status status = OK;

    // try the hashtable lookup
    status = hashTable->lookup(file, PageNo, frameNo);
    if(status == OK) {

        // check pin count
        if(bufTable[frameNo].pinCnt > 0) {

            // decrement pin count
            bufTable[frameNo].pinCnt--;

            // set dirty bit to true if necessary
            if(dirty == true) {
                bufTable[frameNo].dirty = true;
            }

            // finished
            return OK;
        }

        // error, page isn't pinned
        else {
            return PAGENOTPINNED;
        }
    }
    // error, hash lookup doesn't exist in table
    else {
        return HASHNOTFOUND;
    }


    return OK;


}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{


    // allocate a page on the file
    Status status;
    status = file->allocatePage(pageNo);
    if(status != OK)
        return status;

    // allocates frame in buffer
    int frameNo = 0;
    status = allocBuf(frameNo);
    if(status != OK)
        return status;

    // insert new entry into hash table
    status = hashTable->insert(file, pageNo, frameNo);
    if(status != OK)
        return status;

    // set the page on the frame
    bufTable[frameNo].Set(file, pageNo);

    // update page address
    page = &bufPool[frameNo];


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


