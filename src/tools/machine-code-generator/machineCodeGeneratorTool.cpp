// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
//

#include <cstdio>

#include "common/encryption/License.h"
#include "common/network/NetworkUtils.h"

int main(int argc, char* argv[]) {
  using nebula::encryption::License;
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  printf("********* Generating Machine Code... *********\n");
  auto machineCode = License::genMachineCode();
  if (machineCode.empty()) {
    printf("Failed to generate the Machine Code\n");
    return -1;
  }
  printf("********* Machine Code Generation Finished *********\n");
  printf("Machine Code:\n%s\n", machineCode.c_str());
  return 0;
}
