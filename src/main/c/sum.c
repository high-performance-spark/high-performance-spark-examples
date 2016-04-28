#include "sum.h"

int sum(int[] input, int num_elem) {
  int ret = 0;
  for (int c = 0; c < num_elem; c++) {
    ret += input[c];
  }
  return ret;
}
