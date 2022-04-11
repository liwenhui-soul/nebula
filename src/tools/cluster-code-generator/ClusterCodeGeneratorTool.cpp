// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
//

#include <stdio.h>  /* printf */
#include <stdlib.h> /* exit   */

#include "common/encryption/License.h"

using nebula::encryption::License;

DEFINE_string(code,
              "",
              "A list of machine codes has to be given. Each code should be seperated by `,` and "
              "no space included.");

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  // Currently we only support one option: code
  if (FLAGS_code.empty()) {
    LOG(INFO) << "Please specify the machine codes, each code should be seperated by `,` and no "
                 "space included.\nUsage:  --code=<code1,code2,code3>";
    return -1;
  }

  // Split machine codes from the input
  std::vector<std::string> machineCodes;
  folly::split(",", FLAGS_code.c_str(), machineCodes, true);

  printf("Machine codes: ");
  for (auto& ele : machineCodes) {
    printf("%s ", ele.c_str());
  }
  printf("\n");
  printf("********* Generating Cluster Code... *********\n");
  auto clusterCode = License::genClusterCode(machineCodes);

  if (clusterCode.empty()) {
    printf("Failed to generate the Cluster Code\n");
    return EXIT_FAILURE;
  }

  printf("********* Cluster Code Generation Finished *********\n");
  printf("Cluster Code:\n%s\n", clusterCode.c_str());

  return 0;
}
