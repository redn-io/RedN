//#include "memcached.h" 

/* defintion for buckets */
typedef item* ValueType;
typedef unsigned char   TagType;

/*
 * The maximum number of cuckoo operations per insert, 
 * we use 128 in the submission 
 * now change to 500
 */
#define MAX_CUCKOO_COUNT 500


/*
 * the structure of a bucket
 */
#define bucket_size 1

struct Bucket {
    TagType   tags[bucket_size];
    //char      notused[4];
#ifdef REDN
    char	key[3];
    uint64_t	addr;
#endif
    ValueType vals[bucket_size];
}  __attribute__((__packed__));

static struct Bucket* buckets;

#define IS_SLOT_EMPTY(i,j) (buckets[i].tags[j] == 0)

//#define IS_TAG_EQUAL(i,j,tag) ((buckets[i].tags[j] & tagmask) == tag)
#define IS_TAG_EQUAL(bucket,j,tag) ((bucket.tags[j] & tagmask) == tag)


/* associative array */
void assoc2_init(const int hashpower_init);
item *assoc2_find(const char *key, const size_t nkey, const uint32_t hv);
int assoc2_insert(item *item, const uint32_t hv);
void assoc2_delete(const char *key, const size_t nkey, const uint32_t hv);
/* void do_assoc_move_next_bucket(void); */
/* int start_assoc_maintenance_thread(void); */
/* void stop_assoc_maintenance_thread(void); */

void assoc2_destroy(void);
void assoc2_pre_bench(void);
void assoc2_post_bench(void);

uintptr_t assoc2_hash_start(void);
uint64_t assoc2_hash_size(void);
