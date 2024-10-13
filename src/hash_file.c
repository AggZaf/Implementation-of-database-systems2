#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "bf.h"
#include "hash_file.h"

#define MAX_OPEN_FILES 20
#define BLOCK_SIZE 512
#define max_recs (BF_BLOCK_SIZE - sizeof(HT_block_info)) / sizeof(Record)

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}


static HT_info* tableOfIndexes[MAX_OPEN_FILES];    //pinakas apo pointer se anoixta arxeia
int openFiles = 0;                                //gia na kratame ton arithmo ton anoikton arxeion

void record_copy(Record record, void* bucketData, HT_block_info* ptr_block_info); 
int power(int base, int exponent);
int hash(int id, int depth); 
void Hashtable_double(int** hash_table, HT_info *info);

HT_ErrorCode HT_Init() {                          //arxikopoiei ton pinaka me ta anoikta arxeia
  
  for(int i = 0; i < MAX_OPEN_FILES; i++) { 
    tableOfIndexes[i] = NULL;
  }
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {  

  int filedesc;
  CALL_BF(BF_CreateFile(filename));
  CALL_BF(BF_OpenFile(filename, &filedesc));
  //printf("FILE DES\t%d\n",filedesc);

  //dimiurgia tou protou block kai anathesi deikti sta dedomena tou
  BF_Block *block;
  char* data;
  char* data_move;
  BF_Block_Init(&block);
  CALL_BF(BF_AllocateBlock(filedesc, block));
  data = BF_Block_GetData(block);
  data_move = data;

  //desmeusi domis ht_info kai anathesi katallilon timon sta pedia tis
  HT_info ht_info;
  ht_info.fd = filedesc;
  ht_info.global_depth=depth;
  ht_info.numBuckets = power(2,depth);  
  ht_info.sum_of_records=0;      
  ht_info.ht_table = malloc(depth*sizeof(int));
  
  for (int i=0; i<depth; i++){
    ht_info.ht_table[i]=-1;
  }
  //eggrafi tis domis ht_info sto block
  memcpy(data , &ht_info, sizeof(HT_info));
  
  //desmeusi domis ht_block_info kai anathesi katallilon timon sta pedia tis
  HT_block_info ht_block_info;      
  ht_block_info.local_depth = 1;
  ht_block_info.block_id = 0;
  ht_block_info.recsNum =0;
  int block_offset = max_recs * sizeof(Record);   //pairnume to offset tou simeiou pu thelume na grapsume to ht_block_info
  memcpy(data_move+block_offset, &ht_block_info, sizeof(ht_block_info));  //eggrafi tis domis ht_block_info sto telos tu block

  BF_Block_SetDirty(block);         //kano unpin kai katastrefo to block prokeimenu na kleiso to arxeio
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  CALL_BF(BF_CloseFile(filedesc));
  return HT_OK;

}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){    
  
  //an exume ftasei to megisto arithmo anoikton arxeion girna error
  if(openFiles == MAX_OPEN_FILES) {
    fprintf(stderr, "Can't open more files.\n");
    return HT_OK;
  }
  openFiles++;

  CALL_BF(BF_OpenFile(fileName, indexDesc));
  //arxikopoiisi block
  BF_Block *blockinfo;
  BF_Block_Init(&blockinfo);
  CALL_BF(BF_GetBlock(*indexDesc, 0, blockinfo));
  void* data = BF_Block_GetData(blockinfo);
  
  //copy from data to ht_info
  HT_info ht_info;
  memcpy(&ht_info, data, sizeof(HT_info));
  //printf("GLOBAL:\t%d\n", ht_info.global_depth);
  //printf("FD:\t%d\n", ht_info.fd);
  //printf("NUM BUCKETS:\t%d\n", ht_info.numBuckets);
 
  //euresi tis protis diathesimis thesis kai anathesi tis timis tou data se ayton
  int x = 0;
  while (tableOfIndexes[x]!= NULL){
    x++;
  }
  tableOfIndexes[x] = data;

  // kleisimo block 
  BF_Block_SetDirty(blockinfo);
  CALL_BF(BF_UnpinBlock(blockinfo));
  BF_Block_Destroy(&blockinfo);


  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  //vriskoume posa block exei to arxeio
  int blocks_num;
  CALL_BF(BF_GetBlockCounter(indexDesc, &blocks_num));

  BF_Block *block;
  BF_Block_Init(&block);

  //unpin ola ta fortomena block
  for (int i = 0; i < blocks_num; i++) {
    CALL_BF(BF_GetBlock(indexDesc, i, block));
    CALL_BF(BF_UnpinBlock(block));
  }

  BF_Block_Destroy(&block);
  CALL_BF(BF_CloseFile(indexDesc));
  //apodesmeusi tu ht_table
  free(tableOfIndexes[indexDesc]->ht_table);  
  return HT_OK;
  
}




HT_ErrorCode HT_InsertEntry(int indexDesc, Record record){
  //fortonume ta metadata tu arxeiou
  BF_Block* ht_block;
  BF_Block_Init(&ht_block);
  CALL_BF(BF_GetBlock(tableOfIndexes[indexDesc]->fd, 0, ht_block));

  char* data_info = BF_Block_GetData(ht_block);
  HT_info ht_info;
  memcpy(&ht_info, data_info, sizeof(HT_info));
  //printf("FD:%d\tGD:%d\tNB:%d\n", ht_info.fd, ht_info.global_depth, ht_info.numBuckets );
  //ipologismos tou hashvalue
  int hashValue = hash(record.id, ht_info.global_depth);
  printf("HASHVALUE:%d\n", hashValue);
  //fortonume ta metadata tou block
  BF_Block *bucket;
  BF_Block_Init(&bucket);
  void* bucketData;
  HT_block_info ht_block_info;
  ht_block_info.recsNum = 0;
  int block_info_offset = max_recs * sizeof(Record);

  //printf("SLOT:%d", ht_info.ht_table[])
  if (tableOfIndexes[indexDesc]->ht_table[hashValue]==-1){  //an i thesi exei index -1 simainei oti den iparxei kan block opote to dimiurgume
    //printf("PROTO IF\n");
    fflush(stdout);
    CALL_BF(BF_AllocateBlock(ht_info.fd, bucket));          //dimiurgia neou block bucket
    bucketData = BF_Block_GetData(bucket);
    BF_GetBlockCounter(ht_info.fd, &ht_block_info.block_id);   //dino timi sto pedio id tou neou block
    ht_block_info.local_depth = ht_info.global_depth;  
    
    record_copy(record, bucketData, &ht_block_info);  //eggrafi tou record mesa sto bucket  + auksanei kai to block id
    ht_block_info.recsNum++;
    memcpy(data_info+block_info_offset, &ht_block_info, sizeof(HT_block_info));
    //printf("TELOS PROTO IF\n");
    fflush(stdout);
  }
  else{     //an dld iparxei to bucket poy antistoixei sto hashvalue
    //printf("PROTO ELSE\n");
    fflush(stdout);
    CALL_BF(BF_GetBlock(tableOfIndexes[indexDesc]->fd, hashValue, bucket));     //fortose to katallilo bucket
    bucketData = BF_Block_GetData(bucket);
    memcpy(&ht_block_info,bucketData+block_info_offset,sizeof(ht_block_info));  //fortose to block info
    //printf("TELOS PROTO ELSE\n");
    if (ht_block_info.recsNum < max_recs){                          //an iparxei xoros sto block grapsto
      record_copy(record, bucketData, &ht_block_info);
    }
    else{   //an den iparxei xoros sto block
      //memcpy(&ht_block_info,bucketData+block_info_offset,sizeof(ht_block_info));  //fortose to block info
      if (ht_info.global_depth > ht_block_info.local_depth){      //an to topiko mikrotero tu olikou
        HT_Split_Bucket(indexDesc, record, bucketData, &hashValue);
      }
      else{                        //an topiko kai oliko isa
        Hashtable_double(&ht_info.ht_table , &ht_info);           //diplasiazei ton pinaka + auksanei to oliko
        hashValue = hash(record.id, ht_info.global_depth);
        HT_Split_Bucket(indexDesc, record, bucketData, &hashValue);
      }
    }
  }

  BF_Block_SetDirty(ht_block);
  BF_Block_SetDirty(bucket);
  CALL_BF(BF_UnpinBlock(ht_block));
  CALL_BF(BF_UnpinBlock(bucket));
  BF_Block_Destroy(&ht_block);
  BF_Block_Destroy(&bucket);

  return HT_OK;
}

void record_copy(Record record, void* bucketData, HT_block_info* ptr_block_info){ //ti xrisimopoiume gia na antigrapsume ta dedomena enos record
  Record *next_record;
  next_record = bucketData + (ptr_block_info->recsNum * sizeof(Record));

  next_record->id = record.id;
  strncpy(next_record->name, record.name, sizeof(next_record->name));
  next_record->name[sizeof(next_record->name) - 1] = '\0';
  strncpy(next_record->surname, record.surname, sizeof(next_record->surname));
  next_record->surname[sizeof(next_record->surname) - 1] = '\0';
  strncpy(next_record->city, record.city, sizeof(next_record->city));
  next_record->city[sizeof(next_record->city) - 1] = '\0';
  ptr_block_info->recsNum++;  //auksanume to arithmo ton eggrafon sto block

  printf("Egine eggrafh tou record me id:%d\tsurname:%s\tname:%s\tcity:%s\n",next_record->id, next_record->name, next_record->name, next_record->city);
}


HT_ErrorCode HT_Split_Bucket(int indexDesc, Record record, void *oldBucketData, int *hashValue){
    //printf("MESA STO SPLIT\n");fflush(stdout);
    BF_Block *block;
    BF_Block_Init(&block);
    // Get the old bucket datas
    char *data = (char *)oldBucketData;
    
    int block_info_offset = max_recs * sizeof(Record);
    HT_block_info *ptr = (HT_block_info *)data + block_info_offset;

    // Extract necessary information from the old bucket
    int oldLocalDepth = ptr->local_depth;
    int oldRecNum = ptr->recsNum;
    //memcpy(&oldRecNum, data + (BF_BLOCK_SIZE - 4) * sizeof(char), sizeof(int));
    //memcpy(&oldLocalDepth, data + (BF_BLOCK_SIZE - 8) * sizeof(char), sizeof(int));

    // Create two new buckets for splitting
    
    int filedesc;
    filedesc = tableOfIndexes[indexDesc]->fd;
    

    int newBucket1;
    BF_Block *newBlock1;
    BF_Block_Init(&newBlock1);
    BF_AllocateBlock(filedesc, newBlock1);
    BF_GetBlockCounter(filedesc, &newBucket1);
    newBucket1--;


    int newBucket2;
    BF_Block *newBlock2;
    BF_Block_Init(&newBlock2);
    BF_AllocateBlock(filedesc, newBlock2);
    BF_GetBlockCounter(filedesc, &newBucket2);
    newBucket2--;

    char *newData1 = BF_Block_GetData(newBlock1);
    char *newData2 = BF_Block_GetData(newBlock2);

    // Initialize the new buckets
    int newLocalDepth = oldLocalDepth + 1;
    int newRecNum1 = 0, newRecNum2 = 0;

    int newinfooffset = max_recs * sizeof(Record);
    HT_block_info *newptr1 = (HT_block_info *)newData1+newinfooffset;
    memcpy(&newptr1->recsNum, &newRecNum1,sizeof(int));
    memcpy(&newptr1->local_depth, &newLocalDepth,sizeof(int)); 
    
    //memcpy(newData1 + (BF_BLOCK_SIZE - 4) * sizeof(char), &newRecNum1, sizeof(int));
    //memcpy(newData1 + (BF_BLOCK_SIZE - 8) * sizeof(char), &newLocalDepth, sizeof(int));
    HT_block_info *newptr2 = (HT_block_info *)newData2+newinfooffset;
    memcpy(&newptr2->recsNum, &newRecNum2,sizeof(int));
    memcpy(&newptr2->local_depth, &newLocalDepth,sizeof(int));
    
    //memcpy(newData2 + (BF_BLOCK_SIZE - 4) * sizeof(char), &newRecNum2, sizeof(int));
    //memcpy(newData2 + (BF_BLOCK_SIZE - 8) * sizeof(char), &newLocalDepth, sizeof(int));

    BF_Block_SetDirty(newBlock1);
    BF_Block_SetDirty(newBlock2);

    *hashValue = hash(record.id, newLocalDepth);

    // Reinsert records into the two new buckets based on the new hashing
    for (int i = 0; i < oldRecNum; i++) {
        Record currentRecord;
        memcpy(&currentRecord.id, oldBucketData + i * sizeof(Record), sizeof(int));
        memcpy(currentRecord.name, oldBucketData + i * sizeof(Record) + offsetof(Record, name), sizeof(currentRecord.name));
        memcpy(currentRecord.surname, oldBucketData + i * sizeof(Record) + offsetof(Record, surname), sizeof(currentRecord.surname));
        memcpy(currentRecord.city, oldBucketData + i * sizeof(Record) + offsetof(Record, city), sizeof(currentRecord.city));

        int hash2 = hash(currentRecord.id, newLocalDepth);

        if (hash2 == newBucket1) {
            memcpy(newData1 + newRecNum1 * sizeof(Record), &currentRecord.id, sizeof(int));
            memcpy(newData1 + newRecNum1 * sizeof(Record) + offsetof(Record, name), &currentRecord.name, sizeof(currentRecord.name));
            memcpy(newData1 + newRecNum1 * sizeof(Record) + offsetof(Record, surname), &currentRecord.surname, sizeof(currentRecord.surname));
            memcpy(newData1 + newRecNum1 * sizeof(Record) + offsetof(Record, city), &currentRecord.city, sizeof(currentRecord.city));
            newRecNum1++;
            memcpy(newData1 + (BF_BLOCK_SIZE - 4) * sizeof(char), &newRecNum1, sizeof(int));
        } else if (hash2 == newBucket2) {
            memcpy(newData2 + newRecNum2 * sizeof(Record), &currentRecord.id, sizeof(int));
            memcpy(newData2 + newRecNum2 * sizeof(Record) + offsetof(Record, name), &currentRecord.name, sizeof(currentRecord.name));
            memcpy(newData2 + newRecNum2 * sizeof(Record) + offsetof(Record, surname), &currentRecord.surname, sizeof(currentRecord.surname));
            memcpy(newData2 + newRecNum2 * sizeof(Record) + offsetof(Record, city), &currentRecord.city, sizeof(currentRecord.city));
            newRecNum2++;
            memcpy(newData2 + (BF_BLOCK_SIZE - 4) * sizeof(char), &newRecNum2, sizeof(int));
        }
    }

    return HT_OK;
}


HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
    // Get block 0 data to access HT_info
    BF_Block *block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(indexDesc, 0, block));
    char* data = BF_Block_GetData(block);

    HT_info ht_info;
    memcpy(&ht_info, data, sizeof(HT_info));

    for (int i = 1; i <= ht_info.numBuckets; i++) {
        BF_Block *bucket;
        BF_Block_Init(&bucket);
        CALL_BF(BF_GetBlock(indexDesc, i, bucket));
        char* bucketData = BF_Block_GetData(bucket);

        // Read the bucket info
        HT_block_info bucketinfo;
        memcpy(&bucketinfo, bucketData+(max_recs*sizeof(Record)), sizeof(HT_block_info));

        printf("Bucket %d:\n", i );

        // Print entries in the bucket
        for (int j = 0; j < bucketinfo.recsNum; j++) {
            Record record;
            memcpy(&record, bucketData + j * sizeof(Record), sizeof(Record));
            // Print all entries if id is NULL or print entry if ID matches
            if (id == NULL || *id == record.id) {
                printf("ID: %d, Name: %s, Surname: %s, City: %s\n",
                    record.id, record.name, record.surname, record.city);
            }
        }

        // Unpin bucket block
        CALL_BF(BF_UnpinBlock(bucket));
        BF_Block_Destroy(&bucket);
    }
    // Unpin block 0
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return HT_OK;
}

HT_ErrorCode HashStatistics(char* filename){

  int fd;
  BF_Block* infoblock;
  BF_Block_Init(&infoblock); 
  HT_info* info;
  CALL_BF(BF_OpenFile(filename, &fd));
  CALL_BF(BF_GetBlock(fd, 0, infoblock));
  info = (HT_info*)BF_Block_GetData(infoblock);

  int all_records = 0;
  int min = 10;
  int max = 0;
  int num_of_blocks;
  CALL_BF(BF_GetBlockCounter(fd, &num_of_blocks));

  for (int i=1; i<= num_of_blocks; i++){
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fd, i, block));
    void* data = BF_Block_GetData(block);
    HT_block_info* blockinfo = data+(max_recs*sizeof(Record));
    all_records= all_records+blockinfo->recsNum;  //gia to sinolo ton records
    if(blockinfo->recsNum > max){
      max = blockinfo->recsNum;
    }
    if(blockinfo->recsNum < min){
      min = blockinfo->recsNum;
    }
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block); 
    return HT_OK;
  }
  double avg = (double)all_records/num_of_blocks-1;
  printf("Sinolika records:%d\n",all_records);
  printf("Megista records se ena block:%d\n",max);
  printf("Elaxista records se ena block:%d\n",min);
  printf("Mesos oros records se ena block:%f\n",avg);

  CALL_BF(BF_UnpinBlock(infoblock));
  BF_Block_Destroy(&infoblock);
 
  return HT_OK;
}

int hash(int id, int depth) {
  int slots = power(2,depth);
  return id % slots;
}

int power(int base, int exponent){
  int result = 1; // Initialize result to 1
  for (int i = 0; i < exponent; i++){
    result *= base; // Multiply result by base 'exponent' times
  }
  return result;
}

void Hashtable_double(int** hash_table, HT_info *info) {  //
  int oldSize = power(2,info->global_depth);
  info->global_depth++;
  int newSize = power(2, info->global_depth); //oldsize+1
  
  *hash_table = (int *)realloc(*hash_table, newSize * sizeof(int));
  if (*hash_table == NULL) {
    exit(1);
  } else {
    for (; oldSize < newSize; oldSize++) {
      (*hash_table)[oldSize] = -1;
    }
  }
}

HT_ErrorCode test(int indexDesc){
  BF_Block* infoBlock;
  BF_Block_Init(&infoBlock);

  CALL_BF(BF_GetBlock(indexDesc, 0, infoBlock));
  char* data2 = BF_Block_GetData(infoBlock);
  int block_offset = max_recs * sizeof(Record); 
  HT_block_info ht_b_info;
  memcpy(&ht_b_info, data2+block_offset, sizeof(HT_block_info));
  printf("BLOCK ID:\t%d\n", ht_b_info.block_id);
  printf("LOCAL:\t%d\n", ht_b_info.local_depth);
  printf("RECS NUM:\t%d\n", ht_b_info.recsNum);
  printf("NB:\t%d\n", tableOfIndexes[indexDesc]->numBuckets);
  printf("GD:\t%d\n", tableOfIndexes[indexDesc]->global_depth);
  printf("FD:\t%d\n", tableOfIndexes[indexDesc]->fd);
  //CALL_BF(BF_GetBlock(tableOfIndexes[indexDesc]->fd, 0, infoBlock));
  CALL_BF(BF_GetBlock(indexDesc, 1, infoBlock));
  data2 = BF_Block_GetData(infoBlock);
  
  Record* rcptr = (Record*)data2;
  printf("RECORD-ID:%d\n",rcptr->id);

  //printf("EDO\n");
  //fflush(stdout); 
  return HT_OK;
}