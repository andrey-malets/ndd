#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>


#define BLOCK_SIZE 4096
#define ARRAYSIZE(x) (sizeof(x) / sizeof((x)[0]))


int main(int ac, char *av[]) {
  char new_block[BLOCK_SIZE], old_block[BLOCK_SIZE];

  size_t total_blocks = 0;
  size_t different_blocks = 0;

  if (ac != 2) {
    fprintf(stderr, "Usage: %s <output>\n", av[0]);
    return 1;
  }

  const char *output = av[1];
  int ofd = open(output, O_RDWR);
  if (ofd == -1) {
    fprintf(stderr, "Failed to open %s: %s\n", output, strerror(errno));
    return 2;
  }

  ssize_t nread, oread;
  while ((nread = read(STDIN_FILENO, new_block, ARRAYSIZE(new_block)))) {
    if (nread == -1) {
      fprintf(stderr, "Failed read from stdin: %s\n", strerror(errno));
      return 3;
    }
    oread = read(ofd, old_block, nread);
    if (oread == -1) {
      fprintf(stderr, "Failed read from %s: %s\n", output, strerror(errno));
      return 3;
    } else if (oread != nread) {
      fprintf(stderr, "Failed read exactly %zu bytes from %s, read only %zu\n",
              nread, output, oread);
      return 4;
    }

    ++total_blocks;
    if (memcmp(new_block, old_block, nread)) {
      ++different_blocks;
      if (lseek(ofd, -oread, SEEK_CUR) == -1) {
        fprintf(stderr, "Failed seek -%zu bytes in %s: %s\n",
                oread, output, strerror(errno));
        return 5;
      }
      ssize_t owritten = write(ofd, new_block, nread);
      if (owritten == -1) {
        fprintf(stderr, "Failed write to %s: %s", output, strerror(errno));
        return 6;
      } else if (owritten != nread) {
        fprintf(stderr, "Failed write exactly %zu bytes to %s, "
                "wrote only %zu\n", nread, output, owritten);
        return 7;
      }
    }
  }

  close(ofd);

  fprintf(stderr, "Total %d-byte blocks: %zu, different blocks: %zu\n",
          BLOCK_SIZE, total_blocks, different_blocks);

  return 0;
}
