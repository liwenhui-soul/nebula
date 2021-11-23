nebula_add_library(
    drainer_common_obj OBJECT
    DrainerFlags.cpp
    CommonUtils.cpp
)

nebula_add_library(
    drainer_service_handler OBJECT
    DrainerServiceHandler.cpp
    processor/AppendLogProcessor.cpp
    DrainerTaskManager.cpp
    DrainerTask.cpp
)

nebula_add_library(
    drainer_server OBJECT
    DrainerServer.cpp
)

nebula_add_subdirectory(test)
