#include "sum.h"

int sum(int input[], int num_elem) {
  int c, ret = 0;
  for (c = 0; c < num_elem; c++) {
    ret += input[c];
  }
  return ret;
}
