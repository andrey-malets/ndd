#pragma once

struct producer;
struct consumer;

extern struct producer get_socket_reader(const char *spec);
extern struct consumer get_socket_writer(const char *spec);
