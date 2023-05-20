#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/bdev_zone.h"

struct request_context_t {
    char *bdev_name;
    struct spdk_bdev *bdev;
    struct spdk_bdev_desc *bdev_desc;
    struct spdk_io_channel *bdev_io_channel;
    char *buff;
    uint32_t buff_size;
    struct spdk_bdev_io_wait_entry bdev_io_wait;
};
uint64_t g_tick;
/* info about bdev device */
uint64_t g_num_blk = 0;
uint32_t g_block_size = 0;
/* info about zone */
struct spdk_bdev_zone_info zone_info = {0};
uint64_t g_num_zone = 0;
uint64_t g_zone_capacity = 0;
uint64_t g_zone_sz_blk = 0;
uint32_t g_max_open_zone = 0;
uint32_t g_max_active_zone = 0;
uint32_t g_max_append_blk = 0;
uint64_t g_num_io = 0;

static void
usage(void)
{
    printf(" -b <bdev> name of the bdev to use\n");
}

static char *g_bdev_name = "Malloc0"; /* Default bdev name if without -b */
static int
parse_arg(int ch, char *arg)
{
    switch (ch) {
    case 'b':
        g_bdev_name = arg;
        break;
    default:
        return -EINVAL;
    }
    return 0;
}

static void
queue_io_wait_with_cb(struct request_context_t *req_context, spdk_bdev_io_wait_cb cb_fn)
{
    req_context->bdev_io_wait.bdev = req_context->bdev;
    req_context->bdev_io_wait.cb_fn = cb_fn;
    req_context->bdev_io_wait.cb_arg = req_context;
    spdk_bdev_queue_io_wait(req_context->bdev, req_context->bdev_io_channel,
                    &req_context->bdev_io_wait);
}

static void
appstop_error(struct request_context_t *req_context)
{
	spdk_put_io_channel(req_context->bdev_io_channel);
    spdk_bdev_close(req_context->bdev_desc);
    spdk_app_stop(-1);
}

static void
appstop_success(struct request_context_t *req_context)
{
	spdk_put_io_channel(req_context->bdev_io_channel);
    spdk_bdev_close(req_context->bdev_desc);
    spdk_app_stop(0);
}

/* close zone start */
uint64_t close_complete = 0;

static void
close_zone_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    spdk_bdev_free_io(bdev_io);

    if (success) {
        close_complete++;
    } else {
        SPDK_ERRLOG("bdev io read error\n");
        appstop_error(req_context);
    }

    if (close_complete == 5) {
        printf("Close complete\n");
        appstop_success(req_context);
    }
}

static void
close_zone(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    printf("Close zone #10 ~ zone #14...\n");

    for (uint64_t zone = 10; zone < 15; zone++) {
        rc = spdk_bdev_zone_management(req_context->bdev_desc, req_context->bdev_io_channel,
                       zone * g_zone_sz_blk, SPDK_BDEV_ZONE_CLOSE, 
                       close_zone_complete, req_context);
        if (rc == -ENOMEM) {
            SPDK_NOTICELOG("Queueing io\n");
            queue_io_wait_with_cb(req_context, close_zone);
        } else if (rc) {
            SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
            appstop_error(req_context);
        }
    }
}  
/* close zone end */

/* open zone start */
uint64_t open_complete = 0;

static void 
open_zone_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    spdk_bdev_free_io(bdev_io);

    if (success) {
        open_complete++;
    } else {
        SPDK_ERRLOG("bdev io read error\n");
        appstop_error(req_context);
    }

    if (open_complete == 10) {
        printf("Open complete\n");
        //appstop_success(req_context);
        close_zone(req_context);
    }

}

static void
open_zone(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    printf("Open zone #5 ~ zone #14...\n");

    for (uint64_t zone = 5; zone < 15; zone++) {
        rc = spdk_bdev_zone_management(req_context->bdev_desc, req_context->bdev_io_channel,
                       zone * g_zone_sz_blk, SPDK_BDEV_ZONE_OPEN, 
                       open_zone_complete, req_context);
        if (rc == -ENOMEM) {
            SPDK_NOTICELOG("Queueing io\n");
            queue_io_wait_with_cb(req_context, open_zone);
        } else if (rc) {
            SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
            appstop_error(req_context);
        }
    }
}  
/* open zone end */

/* read zone start */ 
uint64_t rz_complete = 0;

static void
read_zone_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    spdk_bdev_free_io(bdev_io);

    if (success) {
        rz_complete++;
        printf("%s", req_context->buff);
    } else {
        SPDK_ERRLOG("bdev io read error\n");
        appstop_error(req_context);
    }

    if (rz_complete == g_num_io) {
        printf("Read complete\n");
        //appstop_success(req_context);
        open_zone(req_context);
    }

}

static void
read_zone(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    printf("Read zone #0 ~ zone #4...\n");
   
    uint64_t num_blocks = 1;
    uint64_t offset_blocks = 0;
    for (uint64_t zone = 0; zone < g_num_io; zone++) {
        offset_blocks = zone * g_zone_sz_blk; 
        // Zero the buffer so that we can use it for reading 
        memset(req_context->buff, 0, req_context->buff_size);
        printf("read: offset_blocks = 0x%lx\n", offset_blocks);
        rc = spdk_bdev_read_blocks(req_context->bdev_desc, req_context->bdev_io_channel,
                                req_context->buff, offset_blocks, num_blocks, 
                                read_zone_complete, req_context);
        if (rc == -ENOMEM) {
            SPDK_NOTICELOG("Queueing io\n");
            queue_io_wait_with_cb(req_context, read_zone);
        } else if (rc) {
            SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
            appstop_error(req_context);
        }
    }
}  
/* read zone end */

/* append zone start */
uint64_t az_complete = 0;

static void
append_zone_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    spdk_bdev_free_io(bdev_io);

    if (success) {
        az_complete++;
    } else {
        SPDK_ERRLOG("bdev io append error: %d\n", EIO);
        appstop_error(req_context);
        return;
    }
    
    if (az_complete == g_num_io) {
        printf("Append complete...\n");
        read_zone(req_context);
       // appstop_success(req_context);
    }
}

static void
append_zone(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    printf("Append & implicit open zone #0 ~ zone #4...\n");
    uint64_t zone_id = 0;
    uint64_t num_blocks = 1;
    uint64_t offset_blocks = 0;
    g_num_io = 5;

    for (uint64_t zone = 0; zone < g_num_io; zone++) {
        offset_blocks = zone * g_zone_sz_blk + 87; // 87 is a random number
        zone_id =spdk_bdev_get_zone_id(req_context->bdev, offset_blocks);
        printf("append: offset_blocks = 0x%lx, zone_id=0x%lx\n", offset_blocks, zone_id);
        rc = spdk_bdev_zone_append(req_context->bdev_desc, req_context->bdev_io_channel,
                                req_context->buff, zone_id, num_blocks, 
                                append_zone_complete, req_context);
        if (rc == -ENOMEM) {
            SPDK_NOTICELOG("Queueing io\n");
            queue_io_wait_with_cb(req_context, append_zone);
        } else if (rc) {
            SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
            appstop_error(req_context);
        }   
    }
}
/* append zone end */

/* reset zone start */
uint64_t reset_complete = 0;

static void
reset_zone_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    spdk_bdev_free_io(bdev_io);
    
    if (success) {
		reset_complete++;
	} else {
        SPDK_ERRLOG("bdev io reset zone error: %d\n", EIO);
        appstop_error(req_context);
        return;
	}

    if (reset_complete == 15) {
        printf("Reset zone complete\n");
        append_zone(req_context);
    }    
}

static void
reset_zone(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    printf("Reset zone #0 ~ zone #14...\n");

    for (uint64_t zone = 0; zone < 15; zone++) {
        rc = spdk_bdev_zone_management(req_context->bdev_desc, req_context->bdev_io_channel,
                       zone * g_zone_sz_blk, SPDK_BDEV_ZONE_RESET, 
                       reset_zone_complete, req_context);

        if (rc == -ENOMEM) {
            SPDK_NOTICELOG("Queueing io\n");
            queue_io_wait_with_cb(req_context, reset_zone);
        } else if (rc) {
            SPDK_ERRLOG("%s error while resetting zone: %d\n", spdk_strerror(-rc), rc);
            appstop_error(req_context);
        }
    }
}
/* reset zone end */

/* get zone info start */
static void
get_zone_info_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    /* Complete the I/O */
    spdk_bdev_free_io(bdev_io);
    
    if (success) {
        printf("Get zone info complete\n");

        g_zone_capacity = zone_info.capacity;
        g_num_zone = spdk_bdev_get_num_zones(req_context->bdev);
        g_zone_sz_blk = spdk_bdev_get_zone_size(req_context->bdev);
        g_max_open_zone = spdk_bdev_get_max_open_zones(req_context->bdev);
        g_max_active_zone = spdk_bdev_get_max_active_zones(req_context->bdev);
        g_max_append_blk = spdk_bdev_get_max_zone_append_size(req_context->bdev);
        printf("[zone info]\n");
        printf("num zone: %lu zones\n", g_num_zone);
        printf("zone size: %lu blocks\n", g_zone_sz_blk);
        printf("zone capacity: %lu blocks\n", g_zone_capacity);
        printf("max open zone: %u zones\n", g_max_open_zone);
        printf("max active zone: %u zones\n", g_max_active_zone);
        printf("max append size: %u blocks\n", g_max_append_blk);

    } else {
        SPDK_ERRLOG("bdev io reset zone error: %d\n", EIO);
        appstop_error(req_context);
        return;
    } 
    reset_zone(req_context); 
}

static void
get_zone_info(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    printf("Get zone info...\n");

    /* get first zone to know zone capacity */
    uint64_t zone_id = 0x4000;
    size_t num_zones = 1;
    rc = spdk_bdev_get_zone_info(req_context->bdev_desc, req_context->bdev_io_channel,
                                zone_id, num_zones, &zone_info,
                                get_zone_info_complete, req_context);
    
    if (rc == -ENOMEM) {
        SPDK_NOTICELOG("Queueing io\n");
        queue_io_wait_with_cb(req_context, get_zone_info);
    } else if (rc) {
        SPDK_ERRLOG("%s error while get zone_info: %d\n", spdk_strerror(-rc), rc);
        appstop_error(req_context);
    }
}
/* get zone info end */


static void
bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		    void *event_ctx)
{
    SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

static void
appstart(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;
    req_context->bdev = NULL;
    req_context->bdev_desc = NULL;

    SPDK_NOTICELOG("Successfully started the application\n");

    /*  Get bdev descriptor to open the bdev by calling spdk_bdev_open_ext() with its name */
    SPDK_NOTICELOG("Opening the bdev %s\n", req_context->bdev_name);
    rc = spdk_bdev_open_ext(req_context->bdev_name, true, bdev_event_cb, NULL,
                            &req_context->bdev_desc);
    if (rc) {
        SPDK_ERRLOG("Could not open bdev: %s\n", req_context->bdev_name);
        spdk_app_stop(-1);
        return;
    }

    /* A bdev pointer is valid while the bdev is opened */
    req_context->bdev = spdk_bdev_desc_get_bdev(req_context->bdev_desc);

    /* Open I/O channel */
    SPDK_NOTICELOG("Opening io channel\n");
    req_context->bdev_io_channel = spdk_bdev_get_io_channel(req_context->bdev_desc);
    if (req_context->bdev_io_channel == NULL) {
        SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
        spdk_bdev_close(req_context->bdev_desc);
        spdk_app_stop(-1);
        return;
    }

    /* Get bdev device info */
    g_num_blk = spdk_bdev_get_num_blocks(req_context->bdev);
    g_block_size = spdk_bdev_get_block_size(req_context->bdev);

    /* Allocate memory for the write buffer.
     * Initialize the write buffer with the string "Hello World!"
     */
    uint32_t buf_align = spdk_bdev_get_buf_align(req_context->bdev);
    req_context->buff_size = g_block_size * spdk_bdev_get_write_unit_size(req_context->bdev);
    req_context->buff = spdk_zmalloc(req_context->buff_size, buf_align, NULL,
                    SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

    if (!req_context->buff) {
        SPDK_ERRLOG("Failed to allocate buffer\n");
        appstop_error(req_context);
        return;
    }
    snprintf(req_context->buff, req_context->buff_size, "%s", "Hello World!\n");

    get_zone_info(req_context);

    /* bdev_iocmd Flow:
     * read x2              (initialize app in  main())
     * zone receive x1      (to get zone capacity)
     * zone send reset x5   (zone #0 ~ zone #14)
     * zone append x5       (zone #0 ~ zone #4)
     * read x5              (zone #0 ~ zone #4)
     * zone send open x5    (zone #5 ~ zone #9)
     * zone send close x5   (zone #10 ~ zone #14)
     */
}

int
main(int argc, char **argv)
{
    struct spdk_app_opts opts = {};
    int rc = 0;
    struct request_context_t req_context = {};

    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "bdev_iocmd";

    /* Parse built-in SPDK command line parameters to enable spdk trace*/
    if ((rc = spdk_app_parse_args(argc, argv, &opts, "b:", NULL, parse_arg,
                      usage)) != SPDK_APP_PARSE_ARGS_SUCCESS) {
        exit(rc);
    }
    req_context.bdev_name = g_bdev_name;

    rc = spdk_app_start(&opts, appstart, &req_context);
    if (rc) {
        SPDK_ERRLOG("ERROR starting application\n");
    }

    spdk_free(req_context.buff);
    spdk_app_fini();
    return rc;
}
