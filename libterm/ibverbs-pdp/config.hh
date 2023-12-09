#pragma once

#ifndef RPC_SERVER_POLL_WITH_EVENT
#define RPC_SERVER_POLL_WITH_EVENT 0
#endif

#ifndef UNIT_SIZE
#define UNIT_SIZE SZ_1M
#endif

#ifndef MAX_RDMA_LENGTH 
#define MAX_RDMA_LENGTH (64ul * SZ_1K)
#endif

#ifndef WRITE_INLINE_SIZE
#define WRITE_INLINE_SIZE SZ_4K
#endif

//#define PAGE_BITMAP_PULL 1 // pull periodically
#define PAGE_BITMAP_UPDATE 0 // incrementally in each request, not pull
#define PAGE_BITMAP_HINT_READ 1
#define MAX_NR_COROUTINES 64
