nebula_add_library(
    drainer_service_handler OBJECT
    DrainerServiceHandler.cpp
    processor/AppendLogProcessor.cpp
)

nebula_add_library(
    drainer_server OBJECT
    DrainerServer.cpp
)
