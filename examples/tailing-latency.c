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
  #define Sleep(ms) usleep(1000 * ms)
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
#include <pthread.h>
#include "libnfs.h"
#include "libnfs-raw.h"
#include "libnfs-raw-mount.h"

void print_usage(const char* prog) {
	fprintf(stderr, "usage: %s WriterFile NfsReaderFile\n", prog);
}

struct PageMem {
	struct timespec t;
	char pad[4096 - sizeof(struct timespec)];
};
struct PageMem wpage, rpage;
size_t rwsize = sizeof(struct PageMem);
size_t sleepms;
struct nfs_context* nfs;
struct nfsfh* fr;

void* read_proc(void* unused) {
	double sum1 = 0, sum2 = 0, sum3 = 0;
	int cur = 0;
	int cnt = atoi(getenv("cnt")?:"100");
	while (1) {
		struct timespec t1, t2, t3, t4;
		clock_gettime(CLOCK_MONOTONIC, &t1);
		while (1) {
			ssize_t n = nfs ? nfs_read(nfs, fr, &rpage, rwsize)
			                : read((intptr_t)fr, &rpage, rwsize);
			t2 = rpage.t;
			clock_gettime(CLOCK_MONOTONIC, &t3);
			if (n < 0) {
				fprintf(stderr, "read(%zd) = %zd : %m\n", rwsize, n);
				exit(1);
			}
			if (n == 0) {
				Sleep(sleepms);
				clock_gettime(CLOCK_MONOTONIC, &t4);
				sum3 += (t4.tv_sec - t3.tv_sec) * 1e9 + (t4.tv_nsec - t3.tv_nsec);
				continue;
			}
			if (n != rwsize) {
				fprintf(stderr, "read(%zd) = %zd : %m\n", rwsize, n);
				exit(1);
			}
			sum1 += (t3.tv_sec - t1.tv_sec) * 1e9 + (t3.tv_nsec - t1.tv_nsec);
			sum2 += (t3.tv_sec - t2.tv_sec) * 1e9 + (t3.tv_nsec - t2.tv_nsec);
			if (++cur == cnt) {
				printf("avg %d : end-to-end %8.4f ms, read-after-write %8.4f ms, sleep %8.4f ms\n",
                        cnt, sum1 / cnt / 1e6, sum2 / cnt / 1e6, sum3 / cnt / 1e6);
				sum1 = sum2 = sum3 = 0;
				cur = 0;
			}
			break;
		}
	}
	return NULL;
}

// Bench mark nfs tailing read latency, WriterFile and NfsReaderFile should be
// the same file in different filesystem's view, NfsReaderFile must be in NFS,
// WriterFile can be in local or in NFS.
//
// If NFS's underlying FS is topling passthrough fuse, sleep time should be 0
//
// Our result is:
// 1. in local loop back network NFS
//    - a server export NFS, run this program at the server mount
//      self-exported NFS.
//    - the result is about 55us, which tail by linux sys NFS is 100us
// 2. in low end commodity network
//    - a server export NFS, run this program on another server which mount
//      the NFS.
//    - the read-after-write latency is:
//      - ~ 130us with    topling passthrough, sleep time is 0
//      - ~ 1~9ms without topling passthrough, sleep time is many
//
int main(int argc, char *argv[]) {
	int flags, dsync = 0, fw, ret;
	char *url = NULL, *server = NULL, *export = NULL, *path = NULL, *strp;
	pthread_t thr;
	sleepms = atoi(getenv("sleepms")?:"10");
	rwsize = atoi(getenv("rwsize")?:"4096");
	if (rwsize < sizeof(struct timespec))
		rwsize = sizeof(struct timespec);
	if (rwsize > sizeof(struct PageMem))
		rwsize = sizeof(struct PageMem);
	printf("rwsize: %zd\n", rwsize);

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
		print_usage(argv[0]);
		return 1;
	}

	if (access(argv[1], O_RDONLY) == 0) {
		fprintf(stderr, "ERROR: file %s exists\n", argv[1]);
		return 1;
	}

  #define SetFlag(f, Default) flags |= atoi(getenv(#f)?:#Default) ? f : 0
  #define StrFlag(f) flags & f ? "|" #f : ""
  #define O_DIRECTW  O_DIRECT
  #define O_DIRECTR  O_DIRECT

	flags = O_CREAT|O_WRONLY;
	SetFlag(O_DSYNC,   1);
	SetFlag(O_DIRECT,  0);
	SetFlag(O_DIRECTW, 1);
	dsync = atoi(getenv("dsync")?:"0");
	printf("write flags: 0%s%s\n", StrFlag(O_DIRECT), StrFlag(O_DSYNC));
	fw = open(argv[1], flags, 0777);
	if (fw < 0) {
		fprintf(stderr, "open(%s, O_CREAT|O_WRONLY%s, 0777) = %m\n",
			argv[1], StrFlag(O_DSYNC));
		return 1;
	}

	url = argv[2];

	// for simple, not parse port!
	// nfs://server/export/dir/filename
	if (strncmp(url, "nfs://", 6) == 0) {
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
			exit(1);
		}
		path = strrchr(export, '/');
		if (export == NULL) {
			fprintf(stderr, "Bad path: %s\n", export);
			free(export);
			free(server);
			exit(1);
		}
		*path++ = '\0'; // for both export and path
		*strp = 0; // for server
		printf("server: %s\n", server);
		printf("export: %s\n", export);
		printf("path  : %s\n", path);

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
			exit(1);
		}
		printf("libnfs open for read success\n");
	}
	else {
		flags = O_RDONLY;
		SetFlag(O_RSYNC  , 0); // has no effect on linux
		SetFlag(O_DIRECT , 0);
		SetFlag(O_DIRECTR, 1);
		printf("read  flags: 0%s%s\n", StrFlag(O_DIRECT), StrFlag(O_RSYNC));
		fr = (struct nfsfh*)(intptr_t)open(url, flags);
		if ((intptr_t)(fr) < 0) {
			fprintf(stderr, "open(%s, O_RDONLY%s%s) = %m\n",
				url, StrFlag(O_RSYNC), StrFlag(O_DIRECT));
			exit(1);
		}
		printf("native open for read success\n");
	}
	if (pthread_create(&thr, NULL, &read_proc, NULL)) {
		perror("pthread_create");
		goto close_files;
	}
	while (1) {
		clock_gettime(CLOCK_MONOTONIC, &wpage.t);
		ssize_t len = write(fw, &wpage, rwsize);
		if (len != rwsize) {
			perror("write");
			goto close_files;
		}
		if (dsync)
			fdatasync(fw);
		Sleep(sleepms);
	}
	pthread_join(thr, NULL);

close_files:
	if (nfs)
		nfs_close(nfs, fr);
	else
		close((intptr_t)(fr));
	close(fw);

finished:
	free(server);
	free(export);
	if (nfs != NULL) {
		nfs_destroy_context(nfs);
	}
	return 0;
}
