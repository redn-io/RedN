#include <time.h>
#include <math.h>
#include <signal.h>
#include <assert.h> 
#include <sys/mman.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <ifaddrs.h>
#include <inttypes.h>
#include "time_stat.h"
#include "agent.h"
#include "hash_bench.h"

#define BUCKET_COUNT 2

#define OFFLOAD_COUNT 1

#define IO_SIZE 65536
//#define IO_SIZE 1024

#define REDN_SINGLE 1
//#define REDN_PARALLEL 1
//#define REDN_SEQUENTIAL 1

#define REDN defined(REDN_SINGLE) || defined(REDN_PARALLEL) || defined(REDN_SEQUENTIAL)

//#define ONE_SIDED 1
//#define TWO_SIDED 1

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))


#define IS_ALIGNED(x, a) (((x) & ((__typeof__(x))(a) - 1)) == 0)

#ifdef __cplusplus
#define ALIGN(x, a)  ALIGN_MASK((x), ((__typeof__(x))(a) - 1))
#else
#define ALIGN(x, a)  ALIGN_MASK((x), ((typeof(x))(a) - 1))
#endif
#define ALIGN_MASK(x, mask)	(((x) + (mask)) & ~(mask))

#if __BIG_ENDIAN__
    #define htonll(x)   (x)
    #define ntohll(x)   (x)
#else
    #define htonll(x)   ((((uint64_t)htonl(x&0xFFFFFFFF)) << 32) + htonl(x >> 32))
    #define ntohll(x)   ((((uint64_t)ntohl(x&0xFFFFFFFF)) << 32) + ntohl(x >> 32))
#endif

enum region_type {
	MR_DATA = 0,
	MR_BUFFER,
	MR_COUNT
};

enum sock_type {
	SOCK_MASTER = 2,
	SOCK_CLIENT,
	SOCK_WORKER
};


#define SHM_PATH "/ifbw_shm"
#define SHM_F_SIZE 128

#define LAT 1

// #define OUTPUT_TO_FILE 1     // write output to file not stdout.

void* create_shm(int *fd, int *res) {
	void * addr;
	*fd = shm_open(SHM_PATH, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (fd < 0) {
		exit(-1);
	}

	*res = ftruncate(*fd, SHM_F_SIZE);
	if (res < 0)
	{
		exit(-1);
	}

	addr = mmap(NULL, SHM_F_SIZE, PROT_WRITE, MAP_SHARED, *fd, 0);
	if (addr == MAP_FAILED){
		exit(-1);
	}

	return addr;
}

void destroy_shm(void *addr) {
	int ret, fd;
	ret = munmap(addr, SHM_F_SIZE);
	if (ret < 0)
	{
		exit(-1);
	}

	fd = shm_unlink(SHM_PATH);
	if (fd < 0) {
		exit(-1);
	}
}

volatile sig_atomic_t stop = 0;

//uint64_t BUFFER_SIZE = 8388608UL; //8 MB

// 8 MB, 8 MB
//uint64_t mr_sizes[MR_COUNT] = {8388608UL, 8388608UL};

// 256 MB, 256 MB
uint64_t mr_sizes[MR_COUNT] = {268265456UL, 268265456UL};

//uint64_t MR_SIZE = 1073741824UL;  //1 GB
//uint64_t MR_SIZE = 10737418240UL; //10 GB
//uint64_t MR_SIZE = 268265456UL; //256 MB

int batch_size = 1;	//default - batching disabled
int sge_count = 1;	//default - 1 scatter/gather element
int use_cas = 0;	//default - compare_and_swap disabled

int psync = 0;		// used for process synchronization

char *portno = "12345";
char *client_portno = "11111";
char *server_portno = "22222";

char *intf = "ib1";

char host[NI_MAXHOST];

int isClient = 0;

struct mr_context regions[MR_COUNT];
struct time_stats *timer;
struct time_stats *timer_total;

static pthread_t offload_thread[BUCKET_COUNT];

int master_sock = 0;
int client_sock[BUCKET_COUNT] = {2, 3};
int worker_sock[BUCKET_COUNT] = {4, 6};

int thread_arg[BUCKET_COUNT] = {0, 0};

int n_client = 0;

pthread_spinlock_t sock_lock;

int temp1_wrid[OFFLOAD_COUNT] = {0};
int temp2_wrid[OFFLOAD_COUNT] = {0};

uint64_t buffer_base_laddr;
uint64_t buffer_base_raddr;
uint64_t data_base_laddr;
uint64_t data_base_raddr;

// count the # of requests received from client
volatile int n_hash_req = 0;

//XXX for testing
#if 1
	struct wqe_ctrl_seg *sr0_ctrl = NULL;
	struct mlx5_wqe_raddr_seg * sr0_raddr = NULL;
	struct mlx5_wqe_data_seg * sr0_data[2] = { NULL };
	struct wqe_ctrl_seg *sr1_ctrl = NULL;
	struct mlx5_wqe_data_seg * sr1_data = NULL;
	struct mlx5_wqe_raddr_seg * sr1_raddr = NULL;
	struct mlx5_wqe_atomic_seg * sr1_atomic = NULL;
	struct wqe_ctrl_seg *sr2_ctrl = NULL;
	struct mlx5_wqe_data_seg * sr2_data = NULL;
	struct mlx5_wqe_raddr_seg * sr2_raddr = NULL;

	int sr0_wrid, sr1_wrid, sr2_wrid;

	struct timespec start;
#endif


void print_seg_data()
{
	if(sr0_data && sr0_raddr) {
		printf("------ AFTER ------\n");
		printf("sr0_data[0]: addr %lu length %u\n", be64toh(sr0_data[0]->addr), ntohl(sr0_data[0]->byte_count));
		printf("sr0_data[1]: addr %lu length %u\n", be64toh(sr0_data[1]->addr), ntohl(sr0_data[1]->byte_count));
		printf("sr0_raddr: raddr %lu\n", ntohll(sr0_raddr->raddr));
		printf("sr1_atomic: compare %lx (original: %lx) swap_add %lx (original: %lx)\n",
				be64toh(sr1_atomic->compare), sr1_atomic->compare, be64toh(sr1_atomic->swap_add), sr1_atomic->swap_add);
		printf("sr1_raddr: raddr %lu\n", ntohll(sr1_raddr->raddr));

		uint32_t sr2_meta = ntohl(sr2_ctrl->opmod_idx_opcode);
		uint16_t idx2 =  ((sr2_meta >> 8) & (UINT_MAX));
		uint8_t opmod2 = ((sr2_meta >> 24) & (UINT_MAX));
		uint8_t opcode2 = (sr2_meta & USHRT_MAX);

		printf("&sr2_ctrl->opmod_idx_opcode %lu\n", (uintptr_t)&sr2_ctrl->opmod_idx_opcode);
		printf("sr2_ctrl: raw %lx idx %u opmod %u opcode %u qpn_ds %x fm_ce_se %x (imm %u)\n", *((uint64_t *)&sr2_ctrl->opmod_idx_opcode), idx2, opmod2, opcode2, ntohl(sr2_ctrl->qpn_ds), ntohl(sr2_ctrl->fm_ce_se), ntohl(sr2_ctrl->imm));
		printf("sr2_data: addr %lu length %u\n", be64toh(sr2_data->addr), ntohl(sr2_data->byte_count));
		printf("*sr2_data->addr = %lu\n", *((uint64_t *)be64toh(sr2_data->addr)));

#if 0
		addr_t base_buffer_addr = mr_local_addr(msg->sockfd, MR_BUFFER);
		printf("buffer1: %lu\n", *((uint64_t *)base_buffer_addr));
		printf("buffer2: %lu\n", *((uint64_t *)(base_buffer_addr + 8)));
#endif
	}
}


static inline unsigned long ALIGN_FLOOR(unsigned long x, int mask)
{
	if (IS_ALIGNED(x, mask))
		return x;
	else
		return ALIGN(x, mask) - mask;
}

void inthand(int signum)
{	
	stop = 1;
}

// call this function to start a nanosecond-resolution timer
struct timespec timer_start()
{
	struct timespec start_time;
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	return start_time;
}

// call this function to end a timer, returning nanoseconds elapsed as a long
long timer_end(struct timespec start_time)
{
	struct timespec end_time;
	long sec_diff, nsec_diff, nanoseconds_elapsed;

	clock_gettime(CLOCK_MONOTONIC, &end_time);

	sec_diff =  end_time.tv_sec - start_time.tv_sec;
	nsec_diff = end_time.tv_nsec - start_time.tv_nsec;

	if(nsec_diff < 0) {
		sec_diff--;
		nsec_diff += (long)1e9;
	}

	nanoseconds_elapsed = sec_diff * (long)1e9 + nsec_diff;

	return nanoseconds_elapsed;
}

double test(struct timespec start)
{
	struct timespec finish;
	clock_gettime(CLOCK_REALTIME, &finish);
 	long seconds = finish.tv_sec - start.tv_sec; 
     	long ns = finish.tv_nsec - start.tv_nsec; 
         
         if (start.tv_nsec > finish.tv_nsec) { // clock underflow 
	 	--seconds; 
	 	ns += 1000000000; 
	     }
	return (double)seconds + (double)ns/(double)1e9;
}

/* Returns new argc */
static int adjust_args(int i, char *argv[], int argc, unsigned del)
{
   if (i >= 0) {
      for (int j = i + del; j < argc; j++, i++)
         argv[i] = argv[j];
      argv[i] = NULL;
      return argc - del;
   }
   return argc;
}

int process_opt_args(int argc, char *argv[])
{
   int dash_d = -1;
restart:
   for (int i = 0; i < argc; i++) {
      //printf("argv[%d] = %s\n", i, argv[i]);
      if (strncmp("-b", argv[i], 2) == 0) {
         batch_size = atoi(argv[i+1]);
         dash_d = i;
	 argc = adjust_args(dash_d, argv, argc, 2);
	 goto restart;
      }
      else if (strncmp("-e", argv[i], 2) == 0) {
	 sge_count = atoi(argv[i+1]);
         dash_d = i;
	 argc = adjust_args(dash_d, argv, argc, 2);
	 goto restart;
      }
      else if (strncmp("-p", argv[i], 2) == 0) {
	 portno = argv[i+1];
         dash_d = i;
	 argc = adjust_args(dash_d, argv, argc, 2);
	 goto restart;
      }
      else if (strncmp("-i", argv[i], 2) == 0) {
	 intf = argv[i+1];
         dash_d = i;
	 argc = adjust_args(dash_d, argv, argc, 2);
	 goto restart;
      } 
      else if (strncmp("-s", argv[i], 2) == 0) {
	 psync = 1;
	 dash_d = i;
	 argc = adjust_args(dash_d, argv, argc, 1);
	 goto restart;
      } 
      else if (strncmp("-cas", argv[i], 4) == 0) {
	 use_cas = 1;
	 dash_d = i;
	 argc = adjust_args(dash_d, argv, argc, 1);
	 goto restart;
      } 
   }

   return argc;
}

uint32_t str_to_size(char* str)
{
	/* magnitude is last character of size */
	char size_magnitude = str[strlen(str)-1];
	/* erase magnitude char */
	str[strlen(str)-1] = 0;
	unsigned long file_size_bytes = strtoull(str, NULL, 0);
	switch(size_magnitude) {
		case 'g':
		case 'G':
			file_size_bytes *= 1024;
		case 'm':
		case 'M':
			file_size_bytes *= 1024;
		case '\0':
		case 'k':
		case 'K':
			file_size_bytes *= 1024;
			break;
		case 'p':
		case 'P':
			file_size_bytes *= 4;
			break;
		case 'b':
		case 'B':
         break;
		default:
			printf("incorrect size format: %s\n", str);
			break;
	}
	return file_size_bytes;
}

uint32_t post_dummy_write(int sockfd, int iosize, int imm)
{
	// send response
	int src_mr = 0;
	int dst_mr = 0;

	uint64_t remote_base_addr = mr_remote_addr(sockfd, dst_mr);
	uint64_t local_base_addr = mr_local_addr(sockfd, src_mr);
	rdma_meta_t *meta =  (rdma_meta_t *) malloc(sizeof(rdma_meta_t)
			+ 1 * sizeof(struct ibv_sge));

	meta->addr = remote_base_addr;
	meta->length = iosize;
	meta->sge_count = 1;
	meta->sge_entries[0].addr = local_base_addr;
	meta->sge_entries[0].length = iosize;
	meta->imm = imm;
	meta->next = NULL;

	if(imm)
		return IBV_WRAPPER_RDMA_WRITE_WITH_IMM_ASYNC(sockfd, meta, src_mr, dst_mr);
	else
		return IBV_WRAPPER_RDMA_WRITE_ASYNC(sockfd, meta, src_mr, dst_mr);
}

uint32_t post_dummy_read(int sockfd, int iosize)
{
	// send response
	int src_mr = 0;
	int dst_mr = 0;

	uint64_t remote_base_addr = mr_remote_addr(sockfd, dst_mr);
	uint64_t local_base_addr = mr_local_addr(sockfd, src_mr);
	rdma_meta_t *meta =  (rdma_meta_t *) malloc(sizeof(rdma_meta_t)
			+ 1 * sizeof(struct ibv_sge));

	meta->addr = remote_base_addr;
	meta->length = iosize;
	meta->sge_count = 1;
	meta->sge_entries[0].addr = local_base_addr;
	meta->sge_entries[0].length = iosize;
	meta->next = NULL;

	return IBV_WRAPPER_RDMA_READ_ASYNC(sockfd, meta, src_mr, dst_mr);
}

void post_dummy_imm(int sockfd, int imm)
{
	// send response
	int src_mr = 0;
	int dst_mr = 0;

	rdma_meta_t *meta =  (rdma_meta_t *) malloc(sizeof(rdma_meta_t)
			+ sizeof(struct ibv_sge));

	meta->addr = 0;
	meta->length = 0;
	meta->sge_count = 0;
	meta->imm = imm;
	meta->next = NULL;

	IBV_WRAPPER_RDMA_WRITE_WITH_IMM_ASYNC(sockfd, meta, src_mr, dst_mr);
}


uint32_t post_read(int sockfd, uint64_t src, uint64_t dst, int iosize, int src_mr, int dst_mr)
{
	uint64_t remote_base_addr = mr_remote_addr(sockfd, dst_mr);
	uint64_t local_base_addr = mr_local_addr(sockfd, src_mr);
	rdma_meta_t *meta =  (rdma_meta_t *) malloc(sizeof(rdma_meta_t)
			+ 1 * sizeof(struct ibv_sge));

	meta->addr = dst;
	meta->length = iosize;
	meta->sge_count = 1;
	meta->sge_entries[0].addr = src;
	meta->sge_entries[0].length = iosize;
	meta->next = NULL;

	printf("reading from dst %lu to src %lu\n", dst, src);

	return IBV_WRAPPER_RDMA_READ_ASYNC(sockfd, meta, src_mr, dst_mr);
}

uint32_t post_noop(int sockfd, int iosize)
{
	int src_mr = 0;
	int dst_mr = 0;
	int signaled = 1;

	uint64_t remote_base_addr = mr_remote_addr(sockfd, dst_mr);
	uint64_t local_base_addr = mr_local_addr(sockfd, src_mr);

	uint64_t rkey = mr_remote_key(sockfd, dst_mr);
	uint64_t lkey = mr_local_key(sockfd, src_mr);

	struct ibv_exp_send_wr *bad_wr = NULL;
	struct ibv_exp_send_wr *wr = (struct ibv_exp_send_wr*) malloc(sizeof(struct ibv_exp_send_wr));
	memset(wr, 0, sizeof(struct ibv_exp_send_wr));

	struct ibv_sge *sge = malloc(sizeof(struct ibv_sge) * 1);
	memset(sge, 0, sizeof(struct ibv_sge) * 1);

	sge[0].addr = local_base_addr;
	sge[0].length = iosize;
	sge[0].lkey = lkey;

	wr->wr_id = IBV_NEXT_WR_ID(sockfd);
	wr->next = NULL;
	wr->sg_list = sge;
	wr->num_sge = 1;
	wr->exp_opcode = IBV_EXP_WR_NOP;
	wr->wr.rdma.remote_addr = remote_base_addr;
	wr->wr.rdma.rkey = rkey;
	//wr->ex.imm_data = htonl(imm);
	
	if(signaled)
		wr->exp_send_flags = IBV_EXP_SEND_SIGNALED;

	//wr->task.cqe_wait.cq_count = ctx->n_posted_ops;

	return IBV_EXP_POST_ASYNC(sockfd, wr);
}

uint32_t post_hash_response(int sockfd, struct hash_bucket *bucket, addr_t remote_addr, uint32_t imm)
{
	// send response
	int src_mr = MR_DATA;
	int dst_mr = MR_DATA;

	uint64_t remote_base_addr = mr_remote_addr(sockfd, dst_mr);
	rdma_meta_t *meta =  (rdma_meta_t *) malloc(sizeof(rdma_meta_t)
			+ 1 * sizeof(struct ibv_sge));

	meta->addr = remote_base_addr;
	meta->length = 8;
	meta->sge_count = 1;
	meta->sge_entries[0].addr = (uintptr_t)&bucket->value[0];
	meta->sge_entries[0].length = IO_SIZE;
	meta->imm = imm;
	meta->next = NULL;

	//printf("--> Send response [key %u value %u]\n", bucket->key[0], bucket->value[0]);

	if(imm)
		return IBV_WRAPPER_RDMA_WRITE_WITH_IMM_ASYNC(sockfd, meta, src_mr, dst_mr);
	else
		return IBV_WRAPPER_RDMA_WRITE_ASYNC(sockfd, meta, src_mr, dst_mr);
}

uint32_t post_hash_read(int sockfd, uint32_t lkey, uint32_t rkey)
{
#if 0
	// send response
	int src_mr = 0;
	int dst_mr = 0;

	uint64_t remote_base_addr = mr_remote_addr(sockfd, dst_mr);
	uint64_t local_base_addr = mr_local_addr(sockfd, src_mr);
	rdma_meta_t *meta =  (rdma_meta_t *) malloc(sizeof(rdma_meta_t)
			+ 2 * sizeof(struct ibv_sge));

	meta->addr = remote_base_addr;
	meta->length = 3 + 8;
	meta->sge_count = 2;
	meta->sge_entries[0].addr = local_base_addr;
	meta->sge_entries[0].length = 3;
	meta->sge_entries[1].addr = local_base_addr;
	meta->sge_entries[1].length = 8;

	meta->next = NULL;

	return IBV_WRAPPER_RDMA_READ_ASYNC(sockfd, meta, src_mr, dst_mr);
#else
	int src_mr = 0;
	int dst_mr = 0;
	int signaled = 1;

	uint64_t remote_base_addr = mr_remote_addr(sockfd, dst_mr);
	uint64_t local_base_addr = mr_local_addr(sockfd, src_mr);

	struct ibv_send_wr *wr = malloc(sizeof(struct ibv_send_wr));
	memset (wr, 0, sizeof (struct ibv_send_wr));

	struct ibv_sge *sge = malloc(sizeof(struct ibv_sge) * 2);
	memset(sge, 0, sizeof(struct ibv_sge) * 2);

	sge[0].addr = local_base_addr;
	sge[0].length = 3;
	sge[0].lkey = lkey;
	sge[1].addr = local_base_addr;
	sge[1].length = 8;
	sge[1].lkey = lkey;

	/* prepare the send work request */
	wr->next = NULL;
	wr->wr_id = IBV_NEXT_WR_ID(sockfd);
	wr->sg_list = sge;
	wr->num_sge = 2;
	wr->wr.rdma.remote_addr = remote_base_addr;
	wr->wr.rdma.rkey = rkey;

	wr->opcode = IBV_WR_RDMA_READ;
	
	if(signaled)
		wr->send_flags = IBV_SEND_SIGNALED;

	return IBV_POST_ASYNC(sockfd, wr);
#endif
}

uint32_t post_get_req_async(int sockfd, uint32_t key, addr_t addr, uint32_t imm, uint32_t offset)
{
#if 1
	struct rdma_metadata *send_meta =  (struct rdma_metadata *)
		calloc(1, sizeof(struct rdma_metadata) + 2 * sizeof(struct ibv_sge));

	printf("--> Send GET [key %u addr %lu]\n", key, addr);

	//post_dummy_imm(master, 0);

	addr_t base_addr = mr_local_addr(sockfd, MR_BUFFER) + offset;
	uint8_t *param1 = (uint8_t *) base_addr; //key
	uint64_t *param2 = (uint64_t *) (base_addr + 4); //addr

	param1[0] = 0;
	param1[1] = 0;
	param1[2] = key;
	*param2 = htonll(addr);

	send_meta->sge_entries[0].addr = (uintptr_t) param1;
	send_meta->sge_entries[0].length = 3;
	send_meta->sge_entries[1].addr = (uintptr_t) param2;
	send_meta->sge_entries[1].length = 8;
	send_meta->length = 11;
	send_meta->sge_count = 2;
	send_meta->addr = 0;
	send_meta->imm = imm;
	return IBV_WRAPPER_SEND_WITH_IMM_ASYNC(sockfd, send_meta, MR_BUFFER, 0);
#else
	struct rdma_metadata *send_meta =  (struct rdma_metadata *)
		calloc(1, sizeof(struct rdma_metadata) + 2 * sizeof(struct ibv_sge));

	printf("--> Send GET [key %u addr %lu]\n", key, addr);

	//post_dummy_imm(master, 0);

	addr_t base_addr = mr_local_addr(sockfd, MR_BUFFER);
	//uint32_t *param1 = (uint32_t *) base_addr; //key
	uint64_t *param2 = (uint64_t *) (base_addr + 4); //addr

	//*param1 = key;
	*param2 = htonll(addr);

	//send_meta->sge_entries[0].addr = (uintptr_t) param1;
	//send_meta->sge_entries[0].length = 3;
	send_meta->sge_entries[0].addr = (uintptr_t) param2;
	send_meta->sge_entries[0].length = 8;
	send_meta->length = 8;
	send_meta->sge_count = 1;
	send_meta->addr = 0;
	IBV_WRAPPER_SEND_ASYNC(sockfd, send_meta, MR_BUFFER, 0);
 	IBV_TRIGGER(master_sock, sockfd, 0);
#endif
}

void post_get_req_sync(int *socks, uint32_t key, addr_t addr, int response_id)
{
	struct timespec start, end;

	addr_t base_addr = mr_local_addr(socks[0], MR_DATA);
	uint64_t *res = (uint64_t *) (base_addr);


#if REDN_PARALLEL
	for(int h=0; h<BUCKET_COUNT; h++)
		//post_get_req_async(socks[h], key, addr + h*sizeof(struct hash_bucket), response_id, h*16);
		post_get_req_async(socks[h], key+h, addr + h*sizeof(struct hash_bucket), response_id, h*16);

	time_stats_start(timer);

	for(int h=0; h<BUCKET_COUNT; h++)
		IBV_TRIGGER(master_sock, socks[h], 0);

	//for(int h=0; h<BUCKET_COUNT; h++)
	//	IBV_AWAIT_RESPONSE(socks[h], response_id);

	while(res[IO_SIZE/8 - 1] != 5556) {
		//printf("res %lu\n", *res);
		//sleep(1);
	}

	time_stats_stop(timer);

	time_stats_print(timer, "Run Complete");

	res[IO_SIZE/8 - 1] = 0;
#elif defined(REDN_SEQUENTIAL)
	for(int h=0; h<BUCKET_COUNT; h++) {
		post_get_req_async(socks[0], key, addr + h*sizeof(struct hash_bucket), response_id + h, h*16);
		//IBV_TRIGGER(master_sock, socks[0], 0);
		//sleep(5);
		//printf("res %lu\n", *res);
	}

	time_stats_start(timer);

	IBV_TRIGGER(master_sock, socks[0], 0);
	//start = timer_start();
//#ifdef LAT
//#endif
	while(res[IO_SIZE/8 - 1] != 5556) {
		//printf("res %lu\n", *res);
		//sleep(1);
	}
//#ifdef LAT
//#endif
	IBV_AWAIT_RESPONSE(socks[0], response_id + BUCKET_COUNT - 1);

	time_stats_stop(timer);
	//printf("await lat: %lu usec\n", timer_end(start)/1000);

	//IBV_AWAIT_WORK_COMPLETION(sockfd, wr_id);

//#ifdef LAT
	time_stats_print(timer, "Run Complete");
//#endif
	//reset 
	res[IO_SIZE/8 - 1] = 0;
#elif defined(ONE_SIDED)

	volatile struct hash_bucket *bucket = NULL;
	uint32_t wr_id = 0;
	addr_t bucket_addr =  mr_remote_addr(socks[0], MR_DATA);


	printf("--> Send GET [key %u addr %lu]\n", key, addr);

	//printf("read from remote addr %lu\n", bucket_addr);
	wr_id = post_read(socks[0], base_addr, bucket_addr, 19 * 6, MR_DATA, MR_DATA);
	IBV_TRIGGER(master_sock, socks[0], 0);

	time_stats_start(timer);

	IBV_AWAIT_WORK_COMPLETION(socks[0], wr_id);

	bucket = (volatile struct hash_bucket *) base_addr;

	//printf("key required %u found %u\n", (uint8_t)key, bucket->key[0]);

	//for(int i=0; i<8; i++)
	//	printf("test[%d] = %lu\n", i, res[i]);

	if(bucket->key[0] == (uint8_t)key) {
		//printf("found key\n");
		wr_id = post_read(socks[0], base_addr,
				bucket_addr + offsetof(struct hash_bucket, value), IO_SIZE, MR_DATA, MR_DATA);
		IBV_TRIGGER(master_sock, socks[0], 0);
		//IBV_AWAIT_WORK_COMPLETION(socks[0], wr_id);

		while(res[IO_SIZE/8 - 1] != 5555 + response_id - 1) {
			//for(int i=0; i<8; i++)
			//	printf("test[%d] = %lu\n", i, res[i]);
			//printf("res %lu\n", res[IO_SIZE/8 - 1]);
			//sleep(1);
		}

		printf("value size %u\n", res[IO_SIZE/8 - 1]);
	}
	else {
		printf("didn't find key required %d. found %d \n", key, bucket->key[0]);
	}

	time_stats_stop(timer);
	//time_stats_print(timer, "Run Complete");

	res[IO_SIZE/8 - 1] = 0;

#else
	post_get_req_async(socks[0], key, addr, response_id, 0);

	IBV_TRIGGER(master_sock, socks[0], 0);

	time_stats_start(timer);

	//start = timer_start();
//#ifdef LAT
//#endif

#if 0
	while(res[0] != 5555 + response_id - 1) {
		//printf("res %lu\n", res[IO_SIZE/8 - 1]);
		//sleep(1);
	}

	res[0] = 0;
#else
	while(res[IO_SIZE/8 - 1] != 5555 + response_id - 1) {
		//printf("res %lu\n", res[IO_SIZE/8 - 1]);
		//sleep(1);
	}

#endif
	//printf("value size %u\n", res[IO_SIZE/8 - 1]);

	//IBV_AWAIT_RESPONSE(socks[0], response_id);
//#ifdef LAT
	time_stats_stop(timer);
//#endif
	//printf("await lat: %lu usec\n", timer_end(start)/1000);


	res[IO_SIZE/8 - 1] = 0;

	//IBV_AWAIT_WORK_COMPLETION(sockfd, wr_id);

//#ifdef LAT
	//time_stats_print(timer, "Run Complete");
//#endif
	//reset 
#endif
	//clock_gettime(CLOCK_MONOTONIC, &start);


}


void post_recv_response(int sockfd)
{
	addr_t base_addr = mr_local_addr(sockfd, MR_DATA);

	// set up RECV for client inputs
	struct rdma_metadata *recv_meta =  (struct rdma_metadata *)
		calloc(1, sizeof(struct rdma_metadata) + 1 * sizeof(struct ibv_sge));

	recv_meta->sge_entries[0].addr = base_addr;
	recv_meta->sge_entries[0].length = 8;
	recv_meta->length = 8;
	recv_meta->sge_count = 1;

	IBV_RECEIVE_SG(sockfd, recv_meta, mr_local_key(sockfd, MR_DATA));
}

int get_ipaddress(char* ip, char* intf){
	struct ifaddrs *interfaces = NULL;
	struct ifaddrs *temp_addr = NULL;
	char *ipAddress = NULL;
	int success = 0;
	int ret = -1;
	// retrieve the current interfaces - returns 0 on success
	success = getifaddrs(&interfaces);
	if (success == 0) {
		// Loop through linked list of interfaces
		temp_addr = interfaces;
		while(temp_addr != NULL) {
			if(temp_addr->ifa_addr->sa_family == AF_INET) {
		// Check if interface is en0 which is the wifi connection on the iPhone
				if(strcmp(temp_addr->ifa_name, intf)==0){
	    				ipAddress=inet_ntoa(((struct sockaddr_in*)temp_addr->ifa_addr)->sin_addr);
					strcpy(ip, ipAddress);
					ret = 0;

				}
			}
			temp_addr = temp_addr->ifa_next;
		}
	}

	// Free memory
	freeifaddrs(interfaces);
	
	return ret;
}

void * offload_hash(void *arg)
{
#if 0
	struct wqe_ctrl_seg *sr0_ctrl = NULL;
	struct mlx5_wqe_raddr_seg * sr0_raddr = NULL;
	struct mlx5_wqe_data_seg * sr0_data[2] = { NULL };
	struct wqe_ctrl_seg *sr1_ctrl = NULL;
	struct mlx5_wqe_data_seg * sr1_data = NULL;
	struct mlx5_wqe_raddr_seg * sr1_raddr = NULL;
	struct mlx5_wqe_atomic_seg * sr1_atomic = NULL;
	struct wqe_ctrl_seg *sr2_ctrl = NULL;
	struct mlx5_wqe_data_seg * sr2_data = NULL;
	struct mlx5_wqe_raddr_seg * sr2_raddr = NULL;
#endif

	struct timespec start, end;

	int id = *((int *)arg);
	int count = OFFLOAD_COUNT;

	int master = master_sock;
	int client = client_sock[id];
	int worker = worker_sock[id];

	//int sr0_wrid, sr1_wrid, sr2_wrid;
	uint64_t base_data_addr = mr_local_addr(worker, MR_DATA);
	uint64_t base_buffer_addr = mr_local_addr(worker, MR_BUFFER);

	struct ibv_mr *client_wq_mr = register_wq(client, client);
	struct ibv_mr *worker_wq_mr = register_wq(worker, worker);

	printf("offload hash with id %d\n", id);

	while(!(rc_ready(client)) || !(rc_ready(worker))) {
        	asm("");
	}

	printf("performing hash offload [client: %d worker: %d]\n", client, worker);

	//post_dummy_write(worker, 0, 9999);

	int count_1 = 8;
	int count_2 = 1;

	// clear cq
	//IBV_WAIT_TILL(lock_sockfds[lock_id], lock_sockfds[lock_id], 0);
	//IBV_TRIGGER(client, lock_sockfds[lock_id], 0);

	for(int k=0; k<count; k++)
	{
		start = timer_start();

		IBV_WAIT_EXPLICIT(worker, client, 1);

		if(k == count - 1)
			IBV_TRIGGER_EXPLICIT(worker, worker, count_1 - 1);
		else
			IBV_TRIGGER_EXPLICIT(worker, worker, count_1);

		printf("remote start: %lu end: %lu\n", mr_remote_addr(worker, MR_DATA), mr_remote_addr(worker, MR_DATA) + mr_sizes[MR_DATA]);
		sr0_wrid = post_hash_read(worker, client_wq_mr->lkey, mr_remote_key(worker, MR_DATA));

		sr1_wrid = IBV_CAS_ASYNC(worker, base_buffer_addr, base_buffer_addr, 0, 1, mr_remote_key(master, MR_BUFFER), client_wq_mr->lkey, 1);
	

		IBV_WAIT_EXPLICIT(worker, worker, 1);

		IBV_TRIGGER_EXPLICIT(worker, client, count_2);
		
		sr2_wrid = post_dummy_write(client, IO_SIZE, k + 1);

		if(k == 0)
			IBV_TRIGGER(master, worker, 2); // trigger first two wrs

#if 1

		// find READ WR
		sr0_ctrl = IBV_FIND_WQE(worker, sr0_wrid);

		if(!sr0_ctrl) {
			printf("Failed to find sr0 seg\n");
			pause();
		}

		uint32_t sr0_meta = ntohl(sr0_ctrl->opmod_idx_opcode);
		uint16_t idx0 =  ((sr0_meta >> 8) & (UINT_MAX));
		uint8_t opmod0 = ((sr0_meta >> 24) & (UINT_MAX));
		uint8_t opcode0 = (sr0_meta & USHRT_MAX);

		//printf("sr0 (READ) segment will be posted to idx #%u\n", idx0);


		// find CAS WR
		sr1_ctrl = IBV_FIND_WQE(worker, sr1_wrid);

		if(!sr1_ctrl) {
			printf("Failed to find sr1 seg\n");
			pause();
		}

		uint32_t sr1_meta = ntohl(sr1_ctrl->opmod_idx_opcode);
		uint16_t idx1 =  ((sr1_meta >> 8) & (UINT_MAX));
		uint8_t opmod1 = ((sr1_meta >> 24) & (UINT_MAX));
		uint8_t opcode1 = (sr1_meta & USHRT_MAX);

		//printf("sr1 (CAS) segment will be posted to idx #%u\n", idx1);

		// find WRITE WR
		sr2_ctrl = IBV_FIND_WQE(client, sr2_wrid);

		if(!sr2_ctrl) {
			printf("Failed to find sr2 seg\n");
			pause();
		}

		uint32_t sr2_meta = ntohl(sr2_ctrl->opmod_idx_opcode);
		uint16_t idx2 =  ((sr2_meta >> 8) & (UINT_MAX));
		uint8_t opmod2 = ((sr2_meta >> 24) & (UINT_MAX));
		uint8_t opcode2 = (sr2_meta & USHRT_MAX);

		//printf("sr2 (WRITE) segment will be posted to idx #%u\n", idx2);


		void *seg0 = ((void*)sr0_ctrl) + sizeof(struct mlx5_wqe_ctrl_seg) + sizeof(struct mlx5_wqe_raddr_seg);

		// need to modify 2 sges
		for(int i=0; i<2; i++) {
			sr0_data[i] = (struct mlx5_wqe_data_seg *) (seg0 + i * sizeof(struct mlx5_wqe_data_seg));
		}

		seg0 = ((void*)sr0_ctrl) + sizeof(struct mlx5_wqe_ctrl_seg);

		sr0_raddr = (struct mlx5_wqe_raddr_seg *) seg0;


		void *seg1 = ((void*)sr1_ctrl) + sizeof(struct mlx5_wqe_ctrl_seg) +
			sizeof(struct mlx5_wqe_atomic_seg) + sizeof(struct mlx5_wqe_raddr_seg);

		sr1_data = (struct mlx5_wqe_data_seg *) seg1;

		seg1 = ((void*)sr1_ctrl) + sizeof(struct mlx5_wqe_ctrl_seg);

		sr1_raddr = (struct mlx5_wqe_raddr_seg *) seg1;

		seg1 = ((void*)sr1_ctrl) + sizeof(struct mlx5_wqe_ctrl_seg) + sizeof(struct mlx5_wqe_raddr_seg);

		sr1_atomic = (struct mlx5_wqe_atomic_seg *) seg1; 


		void *seg2 = ((void*)sr2_ctrl) + sizeof(struct mlx5_wqe_ctrl_seg);

		sr2_raddr = (struct mlx5_wqe_raddr_seg *) seg2;

		seg2 = ((void*)sr2_ctrl) + sizeof(struct mlx5_wqe_ctrl_seg) + sizeof(struct mlx5_wqe_raddr_seg);

		sr2_data = (struct mlx5_wqe_data_seg *) seg2; 

#if 0
		printf("sr0_data: addr %lu length %u\n", be64toh(sr0_data->addr), ntohl(sr0_data->byte_count));
		printf("sr0_raddr: raddr %lu\n", ntohll(sr0_raddr->raddr));
		for(int i=0; i<4; i++) {
			printf("sr1_data[%d]: addr %lu length %u\n", i, be64toh(sr1_data[i]->addr), ntohl(sr1_data[i]->byte_count));
		}
		printf("sr1_raddr: raddr %lu\n", ntohll(sr1_raddr->raddr));
		printf("sr2_wait: cqnum %u count %u\n", ntohl(sr2_en_wait->obj_num), ntohl(sr2_en_wait->pi));
		for(int i=0; i<8; i++)
			printf("sr2_wait: rsvd[%d] = %u\n", i, sr2_en_wait->rsvd0[i]);
#endif

		//XXX uncomment temporarily
		sr0_data[0]->addr = htobe64(((uintptr_t) (&sr2_ctrl->qpn_ds)));
		sr0_data[1]->addr = htobe64(((uintptr_t) (&sr2_data->addr)));

		//XXX for testing
		//sr2_ctrl->qpn_ds = sr2_ctrl->qpn_ds | 0xFFFF0000;
		//sr2_ctrl->qpn_ds = htonl(0xe8000003);

		//XXX might increase latency
		sr0_ctrl->fm_ce_se = htonl(0);

		sr2_ctrl->fm_ce_se = htonl(0);

		//printf("swap data: %lu\n", be64toh(sr1_atomic->swap_add));
		//sr1_atomic->swap_add = htobe64(99999999999);


		sr2_ctrl->opmod_idx_opcode = sr2_ctrl->opmod_idx_opcode | 0x09000000; //SEND

		sr1_atomic->swap_add =  htobe64(*((uint64_t *)&sr2_ctrl->opmod_idx_opcode));

		sr2_ctrl->qpn_ds = htonl((0 << 8) | 3);

		sr2_ctrl->opmod_idx_opcode = sr2_ctrl->opmod_idx_opcode & 0x00FFFFFF; //NOOP

		sr2_ctrl->imm = htonl(k+1);

		sr1_raddr->raddr = htobe64((uintptr_t) &sr2_ctrl->opmod_idx_opcode);
		sr1_atomic->compare = htobe64(*((uint64_t *)&sr2_ctrl->opmod_idx_opcode));
		//sr1_atomic->compare = (*((uint64_t *)&sr2_ctrl->opmod_idx_opcode)) & 0x00FFFFFFFFFFFFFF;


#if 0
		printf("------ BEFORE ------\n");
		printf("sr0_data[0]: addr %lu length %u\n", be64toh(sr0_data[0]->addr), ntohl(sr0_data[0]->byte_count));
		printf("sr0_data[1]: addr %lu length %u\n", be64toh(sr0_data[1]->addr), ntohl(sr0_data[1]->byte_count));
		printf("sr0_raddr: raddr %lu\n", ntohll(sr0_raddr->raddr));
		printf("sr1_atomic: compare %lx (original: %lx) swap_add %lx (original: %lx)\n",
				be64toh(sr1_atomic->compare), sr1_atomic->compare, be64toh(sr1_atomic->swap_add), sr1_atomic->swap_add);
		printf("sr1_raddr: raddr %lu\n", ntohll(sr1_raddr->raddr));

		sr2_meta = ntohl(sr2_ctrl->opmod_idx_opcode);
		idx2 =  ((sr2_meta >> 8) & (UINT_MAX));
		opmod2 = ((sr2_meta >> 24) & (UINT_MAX));
		opcode2 = (sr2_meta & USHRT_MAX);

		printf("&sr2_ctrl->opmod_idx_opcode %lu\n", (uintptr_t)&sr2_ctrl->opmod_idx_opcode);
		printf("sr2_ctrl: raw %lx idx %u opmod %u opcode %u qpn_ds %x fm_ce_se %x (imm %u)\n", *((uint64_t *)&sr2_ctrl->opmod_idx_opcode), idx2, opmod2, opcode2, ntohl(sr2_ctrl->qpn_ds), ntohl(sr2_ctrl->fm_ce_se), ntohl(sr2_ctrl->imm));
		printf("sr2_data: addr %lu length %u\n", be64toh(sr2_data->addr), ntohl(sr2_data->byte_count));
		printf("*sr2_data->addr = %lu\n", *((uint64_t *)be64toh(sr2_data->addr)));
#if 0
		*((uint64_t *)base_buffer_addr) = 8888;
		*((uint64_t *)(base_buffer_addr + 8)) = 9999;
		printf("buffer1: %lu\n", *((uint64_t *)base_buffer_addr));
		printf("buffer2: %lu\n", *((uint64_t *)(base_buffer_addr + 8)));
#endif

#endif

		//IBV_RECEIVE(client, be64toh(sr0_data->addr), 64, 0);
		
		// set up RECV for client inputs
		struct rdma_metadata *recv_meta =  (struct rdma_metadata *)
			calloc(1, sizeof(struct rdma_metadata) + 2 * sizeof(struct ibv_sge));

		recv_meta->sge_entries[0].addr = ((uintptr_t)&sr1_atomic->compare)+1;
		recv_meta->sge_entries[0].length = 3;
		recv_meta->sge_entries[1].addr = (uintptr_t)&sr0_raddr->raddr;
		recv_meta->sge_entries[1].length = 8;
		recv_meta->length = 11;
		recv_meta->sge_count = 2;

		IBV_RECEIVE_SG(client, recv_meta, worker_wq_mr->lkey);




#endif
	
		count_1 += 6;
		count_2 += 1;

		temp1_wrid[k] = sr1_wrid;
		temp2_wrid[k] = sr2_wrid;

		printf("iter lat: %lu usec\n", timer_end(start)/1000);

#if defined(REDN_PARALLEL) || defined(REDN_SEQUENTIAL)
		// rate limit
		while(k - n_hash_req/BUCKET_COUNT > 200)
			ibw_cpu_relax();
#else

#ifdef LAT
		// rate limit
		while(k - n_hash_req > 1000)
			ibw_cpu_relax();
#else
		// rate limit
		while(k - n_hash_req > 1000)
			ibw_cpu_relax();
#endif
		
#endif
	}
}

void test_callback(struct app_context *msg)
{
	//ibw_cpu_relax();
	if(!isClient) {	

		//printf("posting receive imm\n");
		int sock_type = rc_connection_meta(msg->sockfd);

		//XXX do not post receives on master lock socket
		if(sock_type != SOCK_CLIENT)
			IBV_RECEIVE_IMM(msg->sockfd);

		if(sock_type == SOCK_CLIENT)
			n_hash_req++;

#if REDN
		print_seg_data();

#elif defined(TWO_SIDED)

	int sockfd = msg->sockfd;
	addr_t base_addr = mr_local_addr(sockfd, MR_BUFFER);
	addr_t remote_addr = mr_remote_addr(sockfd, MR_BUFFER);


	uint8_t *param1 = (uint8_t*) base_addr;
	uint64_t *param2 = (uint64_t*)(base_addr + 4);

	struct hash_bucket *bucket = (struct hash_bucket *) ntohll(*param2);

	printf("received req: key %u addr %lu\n", param1[2], ntohll(*param2));
	//printf("key required %u has %u\n", param1[2], bucket->key[0]);
	if(param1[2] == bucket->key[0]) {
		post_hash_response(sockfd, bucket, remote_addr, msg->id);
		IBV_TRIGGER(master_sock, sockfd, 0);
	}
	else
		printf("Key doesn't exist!\n");

	// set up RECV for client inputs
	struct rdma_metadata *recv_meta =  (struct rdma_metadata *)
		calloc(1, sizeof(struct rdma_metadata) + 2 * sizeof(struct ibv_sge));

	recv_meta->sge_entries[0].addr = base_addr;
	recv_meta->sge_entries[0].length = 3;
	recv_meta->sge_entries[1].addr = base_addr + 4;
	recv_meta->sge_entries[1].length = 8;
	recv_meta->length = 11;
	recv_meta->sge_count = 2;

	IBV_RECEIVE_SG(sockfd, recv_meta, mr_local_key(sockfd, MR_BUFFER));

#endif

	}
	printf("Received response with id %d (n_req %d)\n", msg->id, n_hash_req);
}

void add_peer_socket(int sockfd)
{
	int sock_type = rc_connection_meta(sockfd);

	printf("ADDING PEER SOCKET %d (type: %d)\n", sockfd, sock_type);

	if(isClient || sock_type != SOCK_CLIENT) { //XXX do not post receives on master socket
		for(int i=0; i<100; i++) {
			IBV_RECEIVE_IMM(sockfd);
		}

		return;
	}

#if REDN
	pthread_spin_lock(&sock_lock);

	int worker = add_connection(host, portno, SOCK_WORKER, 1, IBV_EXP_QP_CREATE_MANAGED_SEND);

	int id = n_client++;

	thread_arg[id] = id;

	client_sock[id] = sockfd;
	worker_sock[id] = worker;

	printf("input id %d to offload_hash\n", id);

	pthread_create(&offload_thread[id], NULL, offload_hash, &thread_arg[id]);

	printf("Setting sockfds [client: %d worker: %d]\n", client_sock[id], worker_sock[id]);

	pthread_spin_unlock(&sock_lock);
#elif defined(TWO_SIDED)

	client_sock[0] = sockfd;
	addr_t base_addr = mr_local_addr(sockfd, MR_BUFFER);

	// set up RECV for client inputs
	struct rdma_metadata *recv_meta =  (struct rdma_metadata *)
		calloc(1, sizeof(struct rdma_metadata) + 2 * sizeof(struct ibv_sge));

	recv_meta->sge_entries[0].addr = base_addr;
	recv_meta->sge_entries[0].length = 3;
	recv_meta->sge_entries[1].addr = base_addr + 4;
	recv_meta->sge_entries[1].length = 8;
	recv_meta->length = 11;
	recv_meta->sge_count = 2;

	IBV_RECEIVE_SG(sockfd, recv_meta, mr_local_key(sockfd, MR_BUFFER));

#endif
	
	return;
}

void remove_peer_socket(int sockfd)
{
	;
}

void init_hash_map(addr_t addr)
{
	printf("---- Initializing hashmap ----\n");

	struct hash_bucket *bucket = (struct hash_bucket*)addr;

	printf("bucket addr %lu\n", addr);
	for(int i=0; i<10; i++) {
		bucket[i].key[0] = i+1000;
		bucket[i].key[1] = 0;
		bucket[i].key[2] = 0;
		bucket[i].addr = htobe64((uintptr_t) &bucket[i].value[0]);
		bucket[i].value[0] = 5555 + i;
		bucket[i].value[IO_SIZE / 8 - 1] = 5555 + i;

		printf("bucket[%d] key=%u addr=%lu\n", i, *((uint32_t *)bucket[i].key), be64toh(bucket[i].addr)); 
	}
}

int main(int argc, char **argv)
{
	//char *portno = "12345";
	void *ptr;
	uint32_t iosize;
	uint32_t transfer_size;
	int n_chain;
	int iters;
	int ret;
	int shm_fd;
	int shm_ret;

	int offload_count = OFFLOAD_COUNT;

	int dev = 0;


	timer = (struct time_stats*) malloc(sizeof(struct time_stats));
	timer_total = (struct time_stats*) malloc(sizeof(struct time_stats));
	int *shm_proc = (int*)create_shm(&shm_fd, &shm_ret);

	pthread_spin_init(&sock_lock, PTHREAD_PROCESS_PRIVATE);

	argc = process_opt_args(argc, argv);

	if(psync) {
		printf("Setting shm_proc to zero\n");
		*shm_proc = 0;
		return 0;
	}

	if (argc != 3 && argc != 1) {
		fprintf(stderr, "usage: %s <peer-address> <iters> [-p <portno>] [-e <sge count>] [-b <batch size>]  (note: run without args to use as server)\n", argv[0]);
		return 1;
	}

	if(argc > 1)
		isClient = 1;

	if(isClient) {
	
		iters = atoi(argv[2]);
#ifdef LAT
		time_stats_init(timer, iters);
#else
		time_stats_init(timer, iters);
		time_stats_init(timer_total, 1);
#endif
		//portno = client_portno;
		//if(transfer_size > MR_SIZE) {
		//	printf("Insufficient memory region size; required %u while MR_SIZE is set to %lu\n", transfer_size, MR_SIZE);
		//	return 1;
		//}
	}
	//else
		//portno = server_portno;

	// allocate dram region
	for(int i=0; i<MR_COUNT; i++) {
		printf("Mapping dram memory: size %lu bytes\n", mr_sizes[i]);
		ret = posix_memalign(&ptr, sysconf(_SC_PAGESIZE), mr_sizes[i]);
		regions[i].type = i;
		regions[i].addr = (uintptr_t) ptr;
		regions[i].length = mr_sizes[i];
		if(ret) {
			printf("Failed to map space for dram memory region\n");
			return 1;
		}
	}

	/*
	//allocate some additional buffers
	for(int i=1; i<BUFFER_COUNT+1; i++) {
		posix_memalign(&mem, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
		regions[i].type = i;
		regions[i].addr = (uintptr_t) mem;
        	regions[i].length = BUFFER_SIZE;	
	}
	*/
		/*typedef struct rdma_metadata {
			addr_t address;
			addr_t total_len;
			int sge_count;
			struct ibv_sge sge_entries[];
		} rdma_meta_t;*/	
 	
	init_rdma_agent(portno, regions, MR_COUNT, 256,
			add_peer_socket, remove_peer_socket, test_callback);

	if(get_ipaddress(host, intf)) {
		printf("Failed to find IP on interface %s\n", intf);
		return EXIT_FAILURE;
	}

	master_sock = add_connection(host, portno, SOCK_MASTER, 1, 0);

	// Run in server mode
	if(!isClient) {

		//master_sock = add_connection("13.13.13.7", portno, SOCK_MASTER, 1, 0);
		//signal(SIGINT, inthand);

		init_hash_map(regions[MR_DATA].addr);

#if 0
		while(!(rc_ready(client_sock)) && !(rc_ready(worker_sock)) && !stop) {
        		asm("");
		}

		sleep(5);

		pthread_create(&offload_thread, NULL, offload_hash, &offload_count);

		sleep(5);
#endif

		//IBV_AWAIT_WORK_COMPLETION(worker_sock, sr1_wrid);
		//printf("read end: %lu\n", timer_end(start));

		//IBV_AWAIT_WORK_COMPLETION(client_sock, sr2_wrid);
		//printf("write end: %lu\n", timer_end(start));

		if(1) {
			int count = 0;
			while(!stop) {
				getchar();
				printf("test\n");
				IBV_TRIGGER(master_sock, client_sock[0], 0);
				//IBV_TRIGGER(unlock, lock_sockfds[lock_id], 1); // trigger dummy unlock
				//print_seg_data();
#if 0
				IBV_AWAIT_RESPONSE(client_sock[0], count+1);
				start = timer_start();

				IBV_AWAIT_WORK_COMPLETION(worker_sock[0], temp1_wrid[count]);
				printf("read end: %lu\n", timer_end(start) / 1000);

				IBV_AWAIT_WORK_COMPLETION(client_sock[0], temp2_wrid[count]);
				printf("write end: %lu\n", timer_end(start) / 1000);

				count++;
#endif
			}
		}
		else {
 			while(!stop) {
				sleep(3);
			}
		}

		free((void *)regions[0].addr);
		return 0;
	}

	//master_sock = add_connection("13.13.13.6", portno, SOCK_MASTER, 1, 0);

	while(!(rc_ready(master_sock)) && !stop) {
        		asm("");
	}

	// Run in client mode
#if REDN_PARALLEL
	for(int h=0; h<BUCKET_COUNT; h++) {
 		client_sock[h] = add_connection(argv[1], portno, SOCK_CLIENT, 1, IBV_EXP_QP_CREATE_MANAGED_SEND);
	}
#else
 	client_sock[0] = add_connection(argv[1], portno, SOCK_CLIENT, 1, IBV_EXP_QP_CREATE_MANAGED_SEND);
#endif
	sleep(10);

	int n_remaining = iters;

	int response_id;

#ifndef LAT
	response_id = 1;
	for(int i=0; i<iters; i++) {
		//XXX make sure to assign different offsets (and that user buffer can store all request params)
		IBV_RECEIVE_IMM(client_sock[0]);
		post_get_req_async(client_sock[0], 1000, mr_remote_addr(client_sock[0], MR_DATA), response_id, i*16);
		response_id++;
	}
#endif

#ifndef LAT
	printf("Waiting for start signal\n");
	*shm_proc += 1;
	while (*shm_proc > 0){
		usleep(100);
	}
	time_stats_start(timer_total);
#endif

	printf("Starting benchmark ...\n");



	if(0) {
		response_id = 1;
		while(!stop) {
			char c = getchar();
			getchar(); // for newline
			//char n = getchar();
			//getchar(); // for newline

			//post_recv_response(client_sock);
			IBV_RECEIVE_IMM(client_sock[0]);

			post_get_req_sync(client_sock, 1001, mr_remote_addr(client_sock[0], MR_DATA), response_id);

			response_id++;



			//if(c == 'l')
			//	post_lock_req(client_sock, lock_id, 0);
			//else if(c == 'u')
			//	post_lock_req(unclient_sock, lock_id, 1);
			//else {
			//	printf("unrecognized input command\n");
			//	exit(-1);
			//}
		}

	}

#ifdef LAT
	response_id = 1;
	for(int i=0; i<iters; i++) {

#if REDN_PARALLEL
		for(int h=0; h<BUCKET_COUNT; h++)
			IBV_RECEIVE_IMM(client_sock[h]);
#else
		IBV_RECEIVE_IMM(client_sock[0]);
#endif

		post_get_req_sync(client_sock, 1000, mr_remote_addr(client_sock[0], MR_DATA), response_id);

		//response_id++;

//#ifdef LAT
		usleep(200);
//#endif
	}

	time_stats_print(timer, "Run Complete");
#else
	response_id = iters;
	IBV_TRIGGER(master_sock, client_sock[0], 0);
	IBV_AWAIT_RESPONSE(client_sock[0], response_id);
#endif

	//pause();

#if 0
	sleep(2);

	time_stats_start(timer);

	IBV_TRIGGER_EXPLICIT(master, worker, 10);

	IBV_AWAIT_WORK_COMPLETION(worker, wr_id);
		
	time_stats_stop(timer);

	time_stats_print(timer, "Run Complete");
#endif

#ifndef LAT
	time_stats_stop(timer_total);
	printf("Throughput: %3.3f ops/s\n",(float)(iters)
			/ ((float) time_stats_get_avg(timer_total)));

	printf("\n");
#endif

	//printf("Throughput: %3.3f MB/s\n",(float)(transfer_size)
	//		/ (1024.0 * 1024.0 * (float) time_stats_get_avg(timer)));

	pause();
	//free(ptr);

	return 0;
}
