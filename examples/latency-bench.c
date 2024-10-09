/*
   Copyright (C) by Lei Peng <peng@topling.cn> 2024

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

#define _FILE_OFFSET_BITS 64
#define _GNU_SOURCE

/* Example program using the highlevel sync interface
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef AROS
#include "aros_compat.h"
#endif

#ifdef WIN32
#include <win32/win32_compat.h>
#pragma comment(lib, "ws2_32.lib")
WSADATA wsaData;
#define PRId64 "ll"
#else
#include <inttypes.h>
#include <string.h>
#include <sys/stat.h>
#ifndef AROS
#include <sys/statvfs.h>
#endif
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <libgen.h>
#include "libnfs.h"
#include "libnfs-raw.h"
#include "libnfs-raw-mount.h"

enum sync_t { nosync, allsync, dsync };

void print_usage(const char* prog) {
	fprintf(stderr, "usage: %s WriterFile NfsReaderFile [Sync(sync|dsync)]\n", prog);
}

// Bench mark nfs tailing read latency, WriterFile and NfsReaderFile should be
// the same file in different filesystem's view, NfsReaderFile must be in NFS,
// WriterFile can be in local or in NFS.
//
// Our result is: in local loop back network NFS
//   -- a server export NFS, run this program at the server mount
//      self-exported NFS.
//   -- the result is about 30us, which tail by linux sys NFS is 100us
//
int main(int argc, char *argv[])
{
	struct nfs_context *nfs = NULL;
	int ret;
    char *export;
    struct nfsfh* fr = NULL;
	const char *url = NULL;
	char *server = NULL, *path = NULL, *strp;
	enum sync_t syn = nosync;
	double sum = 0;
	size_t cur = 0;
	size_t cnt = atoi(getenv("cnt")?:"1000");

#ifdef WIN32
	if (WSAStartup(MAKEWORD(2,2), &wsaData) != 0) {
		printf("Failed to start Winsock2\n");
		exit(10);
	}
#endif

#ifdef AROS
	aros_init_socket();
#endif

	if (argc < 3) {
		fprintf(stderr, "usage: %s WriterFile ReaderFile [Sync(sync|dsync)]\n", argv[0]);
		return 1;
	}

	url = argv[2];

    // for simple, not parse port!
    // nfs://server/export/dir/filename
	if (strncmp(url, "nfs://", 6)) {
		fprintf(stderr, "Invalid URL specified.\n");
		print_usage(argv[0]);
		exit(0);
	}
	server = strdup(url + 6);
	if (server == NULL) {
		fprintf(stderr, "Failed to strdup server string\n");
		exit(10);
	}
	if (server[0] == '/' || server[0] == '\0') {
		fprintf(stderr, "Invalid server string.\n");
		free(server);
		exit(10);
	}
	strp = strchr(server, '/');
	if (strp == NULL) {
		fprintf(stderr, "Invalid URL specified.\n");
		print_usage(argv[0]);
		free(server);
		exit(0);
	}
	export = strdup(strp);
	if (export == NULL) {
		fprintf(stderr, "Failed to strdup server string\n");
		free(server);
		exit(10);
	}
    path = strrchr(export, '/');
	if (export == NULL) {
		fprintf(stderr, "Bad path: %s\n", export);
        free(export);
		free(server);
		exit(10);
	}
    *path++ = '\0'; // for both export and path
	*strp = 0; // for server
    printf("server: %s\n", server);
    printf("export: %s\n", export);
    printf("path  : %s\n", path);

	int fw = open(argv[1], O_CREAT|O_RDWR, 0777);
	if (fw < 0) {
		fprintf(stderr, "open(%s, O_CREAT|O_RDWR, 0777) = %m\n", argv[1]);
		return 1;
	}

	nfs = nfs_init_context();
	if (nfs == NULL) {
		printf("failed to init context\n");
		goto finished;
	}

	ret = nfs_mount(nfs, server, export);
	if (ret != 0) {
 		printf("Failed to mount nfs share : %s\n", nfs_get_error(nfs));
		goto finished;
	}

	ret = nfs_open(nfs, path, O_RDONLY, &fr);
	if (ret != 0) {
		printf("Failed to nfs_open(%s) %s\n", path, nfs_get_error(nfs));
		exit(10);
	}

	if (argc >= 4) {
		if (strcmp(argv[3], "sync")) {
			syn = allsync;
		} else if (strcmp(argv[3], "dsync") == 0) {
			syn = dsync;
		}
	}
	while (1) {
		struct timespec t1, t2, t3;
		clock_gettime(CLOCK_MONOTONIC, &t1);
		write(fw, &t1, sizeof(struct timespec));
		if (syn == allsync) {
			fsync(fw);
		} else if (syn == dsync) {
			fdatasync(fw);
		}
		clock_gettime(CLOCK_MONOTONIC, &t2);
		while (1) {
			ssize_t n = nfs_read(nfs, fr, &t1, sizeof(struct timespec));
			clock_gettime(CLOCK_MONOTONIC, &t3);
			if (n < 0) {
				fprintf(stderr, "read(%zd) = %zd : %m\n", sizeof(struct timespec), n);
				exit(1);
			}
			if (n == 0) {
				usleep(0);
				continue;
			}
			if (n != sizeof(struct timespec)) {
				fprintf(stderr, "read(%zd) = %zd : %m\n", sizeof(struct timespec), n);
				exit(1);
			}
			double di = (t3.tv_sec - t2.tv_sec) * 1e9 + (t3.tv_nsec - t2.tv_nsec);
			sum += di;
			if (++cur == cnt) {
				printf("avg %zd = %8.6f ms\n", cnt, sum / cnt / 1e6);
				sum = 0;
				cur = 0;
			}
			break;
		}
	}
	nfs_close(nfs, fr);
	close(fw);

finished:
	free(server);
	free(export);
	if (nfs != NULL) {
		nfs_destroy_context(nfs);
	}
	return 0;
}
