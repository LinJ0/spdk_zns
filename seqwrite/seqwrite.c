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

/* read start */
static void
read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    if (success) {
        SPDK_NOTICELOG("Read string from bdev : %s\n", req_context->buff);
    } else {
        SPDK_ERRLOG("bdev io read error\n");
    }

    /* Complete the bdev io and close the channel */
    spdk_bdev_free_io(bdev_io);
    spdk_put_io_channel(req_context->bdev_io_channel);
    spdk_bdev_close(req_context->bdev_desc);
    SPDK_NOTICELOG("Stopping app\n");
    spdk_app_stop(success ? 0 : -1);
}

static void
process_read(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    SPDK_NOTICELOG("Reading io\n");
    rc = spdk_bdev_read(req_context->bdev_desc, req_context->bdev_io_channel,
                req_context->buff, 0, req_context->buff_size, read_complete,
                req_context);

    if (rc == -ENOMEM) {
        SPDK_NOTICELOG("Queueing io\n");
        /* In case we cannot perform I/O now, queue I/O */
        req_context->bdev_io_wait.bdev = req_context->bdev;
        req_context->bdev_io_wait.cb_fn = process_read;
        req_context->bdev_io_wait.cb_arg = req_context;
        spdk_bdev_queue_io_wait(req_context->bdev, req_context->bdev_io_channel,
                    &req_context->bdev_io_wait);
    } else if (rc) {
        SPDK_ERRLOG("%s error while reading from bdev: %d\n", spdk_strerror(-rc), rc);
        spdk_put_io_channel(req_context->bdev_io_channel);
        spdk_bdev_close(req_context->bdev_desc);
        spdk_app_stop(-1);
    }
}
/* read end */

/* write start */
static void
write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    /* Complete the I/O */
    spdk_bdev_free_io(bdev_io);

    if (success) {
        SPDK_NOTICELOG("bdev io write completed successfully\n");
    } else {
        SPDK_ERRLOG("bdev io write error: %d\n", EIO);
        spdk_put_io_channel(req_context->bdev_io_channel);
        spdk_bdev_close(req_context->bdev_desc);
        spdk_app_stop(-1);
        return;
    }

    /* Zero the buffer so that we can use it for reading */
    memset(req_context->buff, 0, req_context->buff_size);

    process_read(req_context);
}

static void
process_write(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    SPDK_NOTICELOG("Writing to the bdev\n");
    rc = spdk_bdev_write(req_context->bdev_desc, req_context->bdev_io_channel,
                 req_context->buff, 0, req_context->buff_size, write_complete,
                 req_context);

    if (rc == -ENOMEM) {
        SPDK_NOTICELOG("Queueing io\n");
        /* In case we cannot perform I/O now, queue I/O */
        req_context->bdev_io_wait.bdev = req_context->bdev;
        req_context->bdev_io_wait.cb_fn = process_write;
        req_context->bdev_io_wait.cb_arg = req_context;
        spdk_bdev_queue_io_wait(req_context->bdev, req_context->bdev_io_channel,
                    &req_context->bdev_io_wait);
    } else if (rc) {
        SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
        spdk_put_io_channel(req_context->bdev_io_channel);
        spdk_bdev_close(req_context->bdev_desc);
        spdk_app_stop(-1);
    }
}
/* write end */

/* reset zone start */
static void
reset_zone_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
    struct request_context_t *req_context = cb_arg;

    /* Complete the I/O */
    spdk_bdev_free_io(bdev_io);

    if (!success) {
        SPDK_ERRLOG("bdev io reset zone error: %d\n", EIO);
        spdk_put_io_channel(req_context->bdev_io_channel);
        spdk_bdev_close(req_context->bdev_desc);
        spdk_app_stop(-1);
        return;
    }
    process_write(req_context);
}

static void
reset_all_zone(void *arg)
{
    struct request_context_t *req_context = arg;
    int rc = 0;

    rc = spdk_bdev_zone_management(req_context->bdev_desc, req_context->bdev_io_channel,
                       0, SPDK_BDEV_ZONE_RESET, reset_zone_complete, req_context);

    if (rc == -ENOMEM) {
        SPDK_NOTICELOG("Queueing io\n");
        /* In case we cannot perform I/O now, queue I/O */
        req_context->bdev_io_wait.bdev = req_context->bdev;
        req_context->bdev_io_wait.cb_fn = reset_all_zone;
        req_context->bdev_io_wait.cb_arg = req_context;
        spdk_bdev_queue_io_wait(req_context->bdev, req_context->bdev_io_channel,
                    &req_context->bdev_io_wait);
    } else if (rc) {
        SPDK_ERRLOG("%s error while resetting zone: %d\n", spdk_strerror(-rc), rc);
        spdk_put_io_channel(req_context->bdev_io_channel);
        spdk_bdev_close(req_context->bdev_desc);
        spdk_app_stop(-1);
    }
}
/* reset zone end */

static void
bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		    void *event_ctx)
{
    SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

static void
process_start(void *arg)
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

    /* Allocate memory for the write buffer.
     * Initialize the write buffer with the string "Hello World!"
     */

    req_context->buff_size = spdk_bdev_get_block_size(req_context->bdev) *
                   spdk_bdev_get_write_unit_size(req_context->bdev);
    uint32_t buf_align = spdk_bdev_get_buf_align(req_context->bdev);
    req_context->buff = spdk_dma_zmalloc(req_context->buff_size, buf_align, NULL);
    if (!req_context->buff) {
        SPDK_ERRLOG("Failed to allocate buffer\n");
        spdk_put_io_channel(req_context->bdev_io_channel);
        spdk_bdev_close(req_context->bdev_desc);
        spdk_app_stop(-1);
        return;
    }
    snprintf(req_context->buff, req_context->buff_size, "%s", "Hello World!\n");

    if (spdk_bdev_is_zoned(req_context->bdev)) {
        /* bdev zone information*/
        printf("[zone info]\n");
        uint64_t num_zone = spdk_bdev_get_num_zones(req_context->bdev);
        uint64_t zone_size = spdk_bdev_get_zone_size(req_context->bdev);
        printf("num zone: %lu of zone\n", num_zone);
        printf("zone size: %lu of lba\n", zone_size);

        printf("[zone limitation]\n");
        uint32_t max_open_zone = spdk_bdev_get_max_open_zones(req_context->bdev);
        uint32_t max_active_zone = spdk_bdev_get_max_active_zones(req_context->bdev);
        uint32_t max_append_size = spdk_bdev_get_max_zone_append_size(req_context->bdev);
        printf("max open zone: %u of zone\n", max_open_zone);
        printf("max active zone: %u of zone\n", max_active_zone);
        printf("max append size: %u of lba\n", max_append_size);
       
        reset_all_zone(req_context);
        return;
	}

	process_write(req_context);

}

int
main(int argc, char **argv)
{
    struct spdk_app_opts opts = {};
    int rc = 0;
    struct request_context_t req_context = {};

    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "seqwrite";

    /* Parse built-in SPDK command line parameters to enable spdk trace*/
    if ((rc = spdk_app_parse_args(argc, argv, &opts, "b:", NULL, parse_arg,
                      usage)) != SPDK_APP_PARSE_ARGS_SUCCESS) {
        exit(rc);
    }
    req_context.bdev_name = g_bdev_name;

    rc = spdk_app_start(&opts, process_start, &req_context);
    if (rc) {
        SPDK_ERRLOG("ERROR starting application\n");
    }

    spdk_dma_free(req_context.buff);
    spdk_app_fini();
    return rc;
}
