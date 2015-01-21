
/*
 * To whom it may concern: http://womble.decadent.org.uk/readdir_r-advisory.html
 */

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>
#include <dirent.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#ifdef HAVE_FNMATCH_H
#include <fnmatch.h>
#endif
#include <pthread.h>

#include "db.h"
#include "duc.h"
#include "list.h"
#include "private.h"
#include "threadpool.h"

struct duc_index_req {
	duc *duc;
	struct list *path_list;
	struct list *exclude_list;
	dev_t dev;
	duc_index_flags flags;
	int maxdepth;
};


duc_index_req *duc_index_req_new(duc *duc)
{
	struct duc_index_req *req = duc_malloc(sizeof(struct duc_index_req));
	memset(req, 0, sizeof *req);

	req->duc = duc;

	return req;
}


int duc_index_req_free(duc_index_req *req)
{
	list_free(req->exclude_list, free);
	list_free(req->path_list, free);
	free(req);
	return 0;
}


int duc_index_req_add_path(duc_index_req *req, const char *path)
{
	duc *duc = req->duc;
	char *path_canon = stripdir(path);
	if(path_canon == NULL) {
		duc_log(duc, DUC_LOG_WRN, "Error converting path %s: %s\n", path, strerror(errno));
		duc->err = DUC_E_UNKNOWN;
		if(errno == EACCES) duc->err = DUC_E_PERMISSION_DENIED;
		if(errno == ENOENT) duc->err = DUC_E_PATH_NOT_FOUND;
		return -1;
	}

	list_push(&req->path_list, path_canon);
	return 0;
}


int duc_index_req_add_exclude(duc_index_req *req, const char *patt)
{
	char *pcopy = duc_strdup(patt);
	list_push(&req->exclude_list, pcopy);
	return 0;
}


int duc_index_req_set_maxdepth(duc_index_req *req, int maxdepth)
{
	req->maxdepth = maxdepth;
	return 0;
}


static int match_list(const char *name, struct list *l)
{
	while(l) {
#ifdef HAVE_FNMATCH_H
		if(fnmatch(l->data, name, 0) == 0) return 1;
#else
		if(strstr(name, l->data) == 0) return 1;
#endif
		l = l->next;
	}
	return 0;
}


struct job {
	pthread_mutex_t mutex;
	struct duc_index_req *req;
	int depth;
	thr_pool_t *pool;
	int fd_parent;
	char *path;
	void (*fn_done)(struct job *j, void *ptr);
	void *ptr;

	/* worker data */

	struct duc_dir *dir;
	int subjobs;
	int fd_dir;
	DIR *d;
	off_t file_count;
	off_t dir_count;
	off_t size_total;
	dev_t dev;
	ino_t ino;
};


static struct job *job_new(const char *path)
{
	struct job *j;
	
	j = duc_malloc(sizeof *j);
	memset(j, 0, sizeof(*j));

	pthread_mutex_init(&j->mutex, NULL);
	j->path = duc_strdup(path);
	
	return j;
}


static void job_lock(struct job *j)
{
	pthread_mutex_lock(&j->mutex);
}


static void job_unlock(struct job *j)
{
	pthread_mutex_unlock(&j->mutex);
}


static void job_free(struct job *j)
{
	duc_free(j->path);

	if(j->d > 0) {
		closedir(j->d);
	} else {
		if(j->fd_dir) {
			close(j->fd_dir);
		}
	}

	job_unlock(j);
	duc_free(j);
}


static int check_job_complete(struct job *j)
{
	if(j->subjobs == 0) {
		
		if(j->fn_done) {
			j->fn_done(j, j->ptr);
		}

		if(j->dir) {
			db_write_dir(j->dir);
			duc_dir_close(j->dir);
		}

		job_free(j);

		return 1;
	} else {
		job_unlock(j);
		return 0;
	}
}


static void on_job_done(struct job *j, void *ptr)
{
	struct job *j_parent = ptr;
	assert(j_parent);

	job_lock(j_parent);

	if(j_parent->dir) {
		if(j->dir) {
			duc_dir_add_ent(j_parent->dir, j->path, j->size_total, DT_DIR, j->dev, j->ino);
		}
		j_parent->dir->size_total += j->size_total;
	}

	j_parent->file_count += j->file_count;
	j_parent->dir_count += j->dir_count;
	j_parent->size_total += j->size_total;

	j_parent->subjobs --;

	check_job_complete(j_parent);
}


static void *do_job(void *p)
{
	struct job *j = p;

	job_lock(j);

	struct duc_index_req *req = j->req;
	struct duc *duc = req->duc;
	const char *path = j->path;
	int fd_parent = j->fd_parent;
	int depth = j->depth;

	/* Open dir and read file status */

	j->fd_dir = openat(fd_parent, path, O_RDONLY | O_NOCTTY | O_DIRECTORY | O_NOFOLLOW);
	if(j->fd_dir == -1) {
		duc_log(duc, DUC_LOG_WRN, "Skipping %s: %s\n", path, strerror(errno));
		check_job_complete(j);
		return NULL;
	}

	struct stat st_dir;
	int r = fstat(j->fd_dir, &st_dir);
	if(r == -1) {
		duc_log(duc, DUC_LOG_WRN, "fstat(%s): %s\n", path, strerror(errno));
		check_job_complete(j);
		return NULL;
	}

	j->dev = st_dir.st_dev;
	j->ino = st_dir.st_ino;
	
	j->d = fdopendir(j->fd_dir);
	if(j->d == NULL) {
		duc_log(duc, DUC_LOG_WRN, "fdopendir(%s): %s\n", path, strerror(errno));
		check_job_complete(j);
		return NULL;
	}

	/* Iterate directory entries */

	struct dirent e;
	struct dirent *e_result;

	while( readdir_r(j->d, &e, &e_result) == 0) {

		if(e_result == NULL) break;

		/* Skip . and .. */

		const char *n = e.d_name;
		if(n[0] == '.') {
			if(n[1] == '\0') continue;
			if(n[1] == '.' && n[2] == '\0') continue;
		}

		if(match_list(e.d_name, req->exclude_list)) continue;

		/* Get file info */

		struct stat st;
		int r = fstatat(j->fd_dir, e.d_name, &st, AT_SYMLINK_NOFOLLOW);
		if(r == -1) {
			duc_log(duc, DUC_LOG_WRN, "Error statting %s: %s\n", e.d_name, strerror(errno));
			continue;
		}

		duc_log(duc, DUC_LOG_DMP, "%s\n", e.d_name);

		if(e.d_type == DT_DIR) {

			/* Check if we are allowed to cross file system boundaries */

			if(req->flags & DUC_INDEX_XDEV) {
				if(st.st_dev != req->dev) {
					duc_log(duc, DUC_LOG_WRN, "Skipping %s: not crossing file system boundaries\n", path);
					continue;
				}
			}

			/* Create new job for indexing subdirectory */

			struct job *j2 = job_new(e.d_name);
			j2->req = j->req;
			j2->pool = j->pool;
			j2->fd_parent = j->fd_dir;
			j2->depth = depth + 1;
			j2->fn_done = on_job_done;
			j2->ptr = j;

			if(req->maxdepth == 0 || j->depth < req->maxdepth) {
				j2->dir = duc_dir_new(duc, st.st_dev, st.st_ino);
				j2->dir->dev_parent = st_dir.st_dev;
				j2->dir->ino_parent = st_dir.st_ino;
			}

			j->subjobs ++;

			thr_pool_queue(j2->pool, do_job, j2);

			if(j->dir) {
				j->dir_count ++;
			}

		} else {

			j->file_count ++;
			j->size_total += st.st_size;

			char *name = e.d_name;
			if((req->flags & DUC_INDEX_HIDE_FILE_NAMES) && !S_ISDIR(st.st_mode)) {
				name = "<FILE>";
			}

			if(j->dir) {
				j->dir->size_total += st.st_size;
				duc_dir_add_ent(j->dir, name, st.st_size, e.d_type, st.st_dev, st.st_ino);
			}
		
		}
	}
	
	check_job_complete(j);

	return NULL;
}


static void index_done(struct job *j, void *ptr)
{
	struct duc_index_report *report = ptr;

	printf("all done %ld\n", j->dir->size_total);

	report->file_count = j->file_count;
	report->dir_count = j->dir_count;
	report->size_total = j->size_total;

}


struct duc_index_report *duc_index(duc_index_req *req, const char *path, duc_index_flags flags)
{
	duc *duc = req->duc;

	req->flags = flags;

	/* Canonalize index path */

	char *path_canon = stripdir(path);
	if(path_canon == NULL) {
		duc_log(duc, DUC_LOG_WRN, "Error converting path %s: %s\n", path, strerror(errno));
		duc->err = DUC_E_UNKNOWN;
		if(errno == EACCES) duc->err = DUC_E_PERMISSION_DENIED;
		if(errno == ENOENT) duc->err = DUC_E_PATH_NOT_FOUND;
		return NULL;
	}

	/* Create report */
	
	struct duc_index_report *report = duc_malloc(sizeof(struct duc_index_report));
	memset(report, 0, sizeof *report);
	snprintf(report->path, sizeof(report->path), "%s", path_canon);

	/* Get dev ID and inode of starting path */

	struct stat st;
	lstat(path_canon, &st);
	req->dev = st.st_dev;
	report->dev = st.st_dev;
	report->ino = st.st_ino;

	/* Recursively index subdirectories */

	gettimeofday(&report->time_start, NULL);
	
	thr_pool_t *pool = thr_pool_create(4, 4, 1, NULL);

	struct job *j = job_new(path_canon);
	j->req = req;
	j->pool = pool;
	j->fn_done = index_done;
	j->ptr = report;
	j->dir = duc_dir_new(duc, st.st_dev, st.st_ino);

	thr_pool_queue(pool, do_job, j);
	thr_pool_wait(pool);

	gettimeofday(&report->time_stop, NULL);
	
	/* Store report */

	db_write_report(duc, report);

	free(path_canon);

	return report;
}



int duc_index_report_free(struct duc_index_report *rep)
{
	free(rep);
	return 0;
}


struct duc_index_report *duc_get_report(duc *duc, size_t id)
{
	size_t indexl;

	char *index = db_get(duc->db, "duc_index_reports", 17, &indexl);
	if(index == NULL) return NULL;

	int report_count = indexl / PATH_MAX;
	if(id >= report_count) return NULL;

	char *path = index + id * PATH_MAX;

	size_t rlen;
	struct duc_index_report *r = db_get(duc->db, path, strlen(path), &rlen);

	free(index);

	return r;
} 


/*
 * End
 */

