#ifndef _VERSION_H_
#define _VERSION_H_

#include <stdio.h>

#define SOFT_VERSION "V1.0_20250311"

void print_version(){
    fprintf(stdout, "=================================================================\n");
    fprintf(stdout, "===================Soft Version: %s===================\n", SOFT_VERSION);
    fprintf(stdout, "=================================================================\n");
}

#endif 