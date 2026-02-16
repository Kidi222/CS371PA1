/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/*
Please specify the group members here
# Student #1:
# Student #2:
# Student #3:
*/

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

typedef struct {
    int epoll_fd;
    int socket_fd;
    long long total_rtt;   /* microseconds */
    long total_messages;
    float request_rate;    /* messages/sec */
} client_thread_data_t;

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return -1;
    return 0;
}

static long long timeval_diff_us(const struct timeval *start, const struct timeval *end) {
    long long s = (long long)start->tv_sec * 1000000LL + (long long)start->tv_usec;
    long long e = (long long)end->tv_sec * 1000000LL + (long long)end->tv_usec;
    return e - s;
}

static ssize_t send_all_blocking(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        sent += (size_t)n;
    }
    return (ssize_t)sent;
}

static ssize_t recv_all_blocking(int fd, void *buf, size_t len) {
    char *p = (char *)buf;
    size_t recvd = 0;
    while (recvd < len) {
        ssize_t n = recv(fd, p + recvd, len - recvd, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return 0; /* peer closed */
        recvd += (size_t)n;
    }
    return (ssize_t)recvd;
}

void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;

    struct epoll_event ev, events[MAX_EVENTS];

    char send_buf[MESSAGE_SIZE];
    memcpy(send_buf, "ABCDEFGHIJKLMNOP", MESSAGE_SIZE);

    char recv_buf[MESSAGE_SIZE];
    struct timeval start, end;

    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN | EPOLLRDHUP;
    ev.data.fd = data->socket_fd;
    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &ev) < 0) die("epoll_ctl client add");

    data->total_rtt = 0;
    data->total_messages = 0;
    data->request_rate = 0.0f;

    for (int i = 0; i < num_requests; i++) {
        if (gettimeofday(&start, NULL) < 0) die("gettimeofday start");

        if (send_all_blocking(data->socket_fd, send_buf, MESSAGE_SIZE) < 0) die("client send");

        /* wait until socket readable, then read exactly 16 bytes */
        for (;;) {
            int n = epoll_wait(data->epoll_fd, events, MAX_EVENTS, -1);
            if (n < 0) {
                if (errno == EINTR) continue;
                die("epoll_wait client");
            }

            int got = 0;
            for (int k = 0; k < n; k++) {
                if (events[k].data.fd != data->socket_fd) continue;

                if (events[k].events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                    fprintf(stderr, "client: server closed connection\n");
                    close(data->socket_fd);
                    return NULL;
                }

                if (events[k].events & EPOLLIN) {
                    ssize_t r = recv_all_blocking(data->socket_fd, recv_buf, MESSAGE_SIZE);
                    if (r <= 0) {
                        fprintf(stderr, "client: recv failed/closed\n");
                        close(data->socket_fd);
                        return NULL;
                    }
                    got = 1;
                    break;
                }
            }

            if (got) break;
        }

        if (gettimeofday(&end, NULL) < 0) die("gettimeofday end");

        long long rtt = timeval_diff_us(&start, &end);
        if (rtt < 0) rtt = 0;

        data->total_rtt += rtt;
        data->total_messages += 1;
    }

    if (data->total_rtt > 0) {
        data->request_rate = (float)((double)data->total_messages * 1000000.0 / (double)data->total_rtt);
    } else {
        data->request_rate = 0.0f;
    }

    return NULL;
}

void run_client() {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) != 1) die("inet_pton");

    for (int i = 0; i < num_client_threads; i++) {
        int s = socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0) die("socket");

        if (connect(s, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) die("connect");

        int ep = epoll_create1(0);
        if (ep < 0) die("epoll_create1 client");

        thread_data[i].socket_fd = s;
        thread_data[i].epoll_fd = ep;
        thread_data[i].total_rtt = 0;
        thread_data[i].total_messages = 0;
        thread_data[i].request_rate = 0.0f;
    }

    for (int i = 0; i < num_client_threads; i++) {
        if (pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]) != 0) die("pthread_create");
    }

    long long total_rtt = 0;
    long total_messages = 0;
    double total_request_rate = 0.0;

    for (int i = 0; i < num_client_threads; i++) {
        pthread_join(threads[i], NULL);
        total_rtt += thread_data[i].total_rtt;
        total_messages += thread_data[i].total_messages;
        total_request_rate += (double)thread_data[i].request_rate;

        close(thread_data[i].socket_fd);
        close(thread_data[i].epoll_fd);
    }

    if (total_messages > 0) {
        printf("Average RTT: %lld us\n", total_rtt / (long long)total_messages);
    } else {
        printf("Average RTT: 0 us\n");
    }
    printf("Total Request Rate: %f messages/s\n", total_request_rate);
}

/* -------- server -------- */

typedef struct {
    int fd;
    int is_listener;
    int want_out;
    size_t out_len;
    size_t out_off;
    char out_buf[4096];
} ep_item_t;

static int ep_update(int epfd, ep_item_t *it) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = it;
    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
    if (it->want_out) ev.events |= EPOLLOUT;
    return epoll_ctl(epfd, EPOLL_CTL_MOD, it->fd, &ev);
}

static int ep_add(int epfd, ep_item_t *it, uint32_t events) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.ptr = it;
    ev.events = events;
    return epoll_ctl(epfd, EPOLL_CTL_ADD, it->fd, &ev);
}

static void close_and_free(int epfd, ep_item_t *it) {
    if (!it) return;
    epoll_ctl(epfd, EPOLL_CTL_DEL, it->fd, NULL);
    close(it->fd);
    free(it);
}

static void try_flush(int epfd, ep_item_t *it) {
    while (it->out_off < it->out_len) {
        ssize_t n = send(it->fd, it->out_buf + it->out_off, it->out_len - it->out_off, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                it->want_out = 1;
                ep_update(epfd, it);
                return;
            }
            close_and_free(epfd, it);
            return;
        }
        if (n == 0) {
            close_and_free(epfd, it);
            return;
        }
        it->out_off += (size_t)n;
    }

    /* fully flushed */
    it->out_len = 0;
    it->out_off = 0;
    if (it->want_out) {
        it->want_out = 0;
        ep_update(epfd, it);
    }
}

void run_server() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) die("socket listen");

    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) die("setsockopt");

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &addr.sin_addr) != 1) die("inet_pton server");

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) die("bind");
    if (listen(listen_fd, 512) < 0) die("listen");

    if (set_nonblocking(listen_fd) < 0) die("set_nonblocking listen");

    int epfd = epoll_create1(0);
    if (epfd < 0) die("epoll_create1 server");

    ep_item_t *listener = (ep_item_t *)calloc(1, sizeof(ep_item_t));
    if (!listener) die("calloc listener");
    listener->fd = listen_fd;
    listener->is_listener = 1;

    if (ep_add(epfd, listener, EPOLLIN | EPOLLET) < 0) die("epoll_ctl add listen");

    struct epoll_event events[MAX_EVENTS];

    while (1) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait server");
        }

        for (int i = 0; i < n; i++) {
            ep_item_t *it = (ep_item_t *)events[i].data.ptr;
            uint32_t ev = events[i].events;

            if (!it) continue;

            if (it->is_listener) {
                for (;;) {
                    struct sockaddr_in caddr;
                    socklen_t clen = sizeof(caddr);
                    int cfd = accept(listen_fd, (struct sockaddr *)&caddr, &clen);
                    if (cfd < 0) {
                        if (errno == EINTR) continue;
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        die("accept");
                    }

                    if (set_nonblocking(cfd) < 0) {
                        close(cfd);
                        continue;
                    }

                    ep_item_t *cit = (ep_item_t *)calloc(1, sizeof(ep_item_t));
                    if (!cit) {
                        close(cfd);
                        continue;
                    }
                    cit->fd = cfd;
                    cit->is_listener = 0;
                    cit->want_out = 0;
                    cit->out_len = 0;
                    cit->out_off = 0;

                    if (ep_add(epfd, cit, EPOLLIN | EPOLLRDHUP | EPOLLET) < 0) {
                        close(cfd);
                        free(cit);
                        continue;
                    }
                }
                continue;
            }

            if (ev & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                close_and_free(epfd, it);
                continue;
            }

            if (ev & EPOLLOUT) {
                try_flush(epfd, it);
                continue;
            }

            if (ev & EPOLLIN) {
                for (;;) {
                    char buf[4096];
                    ssize_t r = recv(it->fd, buf, sizeof(buf), 0);
                    if (r < 0) {
                        if (errno == EINTR) continue;
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        close_and_free(epfd, it);
                        break;
                    }
                    if (r == 0) {
                        close_and_free(epfd, it);
                        break;
                    }

                    /* queue or send echo */
                    if (it->out_len == 0) {
                        ssize_t s = send(it->fd, buf, (size_t)r, MSG_NOSIGNAL);
                        if (s < 0) {
                            if (errno == EINTR) {
                                /* treat as partial 0, queue all */
                                s = 0;
                            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                s = 0;
                            } else {
                                close_and_free(epfd, it);
                                break;
                            }
                        }

                        size_t sent = (s > 0) ? (size_t)s : 0;
                        if (sent < (size_t)r) {
                            size_t rem = (size_t)r - sent;
                            if (rem > sizeof(it->out_buf)) {
                                close_and_free(epfd, it);
                                break;
                            }
                            memcpy(it->out_buf, buf + sent, rem);
                            it->out_len = rem;
                            it->out_off = 0;
                            it->want_out = 1;
                            ep_update(epfd, it);
                        }
                    } else {
                        /* already have pending: append if room */
                        size_t pending = it->out_len - it->out_off;
                        if (pending + (size_t)r > sizeof(it->out_buf)) {
                            close_and_free(epfd, it);
                            break;
                        }

                        /* compact to beginning if needed */
                        if (it->out_off > 0 && pending > 0) {
                            memmove(it->out_buf, it->out_buf + it->out_off, pending);
                        }
                        it->out_off = 0;
                        it->out_len = pending;
                        memcpy(it->out_buf + it->out_len, buf, (size_t)r);
                        it->out_len += (size_t)r;

                        it->want_out = 1;
                        ep_update(epfd, it);
                        try_flush(epfd, it);
                    }
                }
            }
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "server") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        run_server();
    } else if (argc > 1 && strcmp(argv[1], "client") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        if (argc > 4) num_client_threads = atoi(argv[4]);
        if (argc > 5) num_requests = atoi(argv[5]);
        run_client();
    } else {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }
    return 0;
}
