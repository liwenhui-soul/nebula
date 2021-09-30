if(${CMAKE_SYSTEM_NAME} MATCHES "(FreeBSD|Windows)")
	MESSAGE(FATAL_ERROR "Only Linux|Darwin is supported")
endif ()
