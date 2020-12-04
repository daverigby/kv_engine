add_executable_with_pch(mcstat mcstat.cc $<TARGET_OBJECTS:mc_program_utils>)
target_link_libraries(mcstat mc_client_connection platform)
add_sanitizers(mcstat)
install(TARGETS mcstat RUNTIME DESTINATION bin)
