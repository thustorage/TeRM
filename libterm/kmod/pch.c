#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/proc_fs.h>
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/pagemap.h>

#define PROC_NAME "pch"

// taken from Linux source.
static struct page *find_get_incore_page(struct address_space *mapping, pgoff_t index)
{
    struct page *page = pagecache_get_page(mapping, index, FGP_ENTRY | FGP_HEAD, 0);
    if (!page) 
        return page;
    if (!xa_is_value(page)) {
        return find_subpage(page, index);
    }
    return NULL;
}

static unsigned char mincore_page(struct address_space *mapping, pgoff_t index)
{
    struct page *page = find_get_incore_page(mapping, index);
    unsigned char val = 0;

    if (page) {
        if (PageUptodate(page)) {
            val |= (1 << 0);
        }
        if (PageDirty(page)) {
            val |= (1 << 1);
        }
        put_page(page);
    }

    return val;
}


static ssize_t
pch_read(struct file *file, char __user *buf, size_t count, loff_t *pos)
{
    struct file *mmap_fp = file->private_data;

    uint8_t *kern_buf = kvzalloc(PAGE_SIZE, GFP_KERNEL);
    if (!kern_buf) {
        return -EINVAL;
    }

    for (size_t iter_offset = 0; iter_offset < count; iter_offset += PAGE_SIZE) {
        size_t iter_count = min(count - iter_offset, PAGE_SIZE);
        pgoff_t start_idx = *pos + iter_offset;
        pgoff_t last_idx = start_idx + iter_count - 1;

        size_t off = 0;

#if 0
        XA_STATE(xas, &mmap_fp->f_mapping->i_pages, start_idx);
        void *entry;
        rcu_read_lock();
        // entry: is it page or folio? whatever.
        xas_for_each(&xas, entry, last_idx) {
            uint8_t v = 1;
            if (xas_retry(&xas, entry) || xa_is_value(entry)) {
                v = 0;
            }
            kern_buf[off] = v;
            off++;
        }

        rcu_read_unlock();
#else
        for (pgoff_t idx = start_idx; idx <= last_idx; idx++) {
            kern_buf[off] = mincore_page(mmap_fp->f_mapping, idx);
            off++;
        }
#endif        

        if (copy_to_user(buf + iter_offset, kern_buf, iter_count)) {
            pr_err("failed to copy_to_user\n");
            kvfree(kern_buf);
            return -EINVAL;
        }
    }
    kvfree(kern_buf);
    *pos += count;
    return count;
}

static ssize_t
pch_write(struct file *file, const char __user *buf, size_t count, loff_t *pos)
{
    char path[32];
    struct file *mmap_fp;

    if (file->private_data) {
        pr_err("private has been set.\n");
        return -EINVAL;
    }

    if (count > 16) {
        pr_err("path too long\n");
        return -EINVAL;
    }

    if (copy_from_user(path, buf, count)) {
        pr_err("fail to copy from user.\n");
        return -EINVAL;
    }
    path[count] = '\0';

    mmap_fp = filp_open(path, O_RDWR | O_LARGEFILE, 0);
    if (IS_ERR(mmap_fp)) {
        pr_err("fail to open %s\n", path);
        return PTR_ERR(mmap_fp);
    }

    // pr_info("pch: %s\n", path);

    file->private_data = mmap_fp;

    return count;
}

static int pch_release(struct inode *inode, struct file *file)
{
    if (file->private_data) {
        filp_close(file->private_data, 0);
    }
    return 0;
}

static const struct proc_ops pch_ops = {
    .proc_read = pch_read,
    .proc_write = pch_write,
    .proc_release = pch_release,
};

static int __init pch_init(void)
{
    struct proc_dir_entry *entry;

    entry = proc_create(PROC_NAME, S_IFREG | S_IRUGO, NULL, &pch_ops);
    if (!entry) {
        pr_err("failed to create /proc/" PROC_NAME "\n");
        return -ENOMEM;
    }

    return 0;
}

static void __exit pch_exit(void)
{
    remove_proc_entry(PROC_NAME, NULL);
}

module_init(pch_init);
module_exit(pch_exit);

MODULE_LICENSE("GPL");
