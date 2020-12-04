add_executable_with_pch(mclogsplit mclogsplit.cc)
target_link_libraries(mclogsplit dirutils mcd_util)
add_sanitizers(mclogsplit)
install(TARGETS mclogsplit RUNTIME DESTINATION bin)
